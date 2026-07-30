# Справочник: gRPC Admin API (v1.16.0)

Справочник по gRPC-интерфейсу динамического управления квотами и правилами ACL без перезапуска брокера.

> **Diátaxis:** Справочник (Reference) — описание RPC, аутентификации, идемпотентности. За примерами клиента обращайтесь к `examples/admin/`.

---

## 1. Общие сведения

| Свойство | Значение |
| :--- | :--- |
| Транспорт | gRPC (HTTP/2 over TCP) |
| Адрес | `cfg.admin.addr` (по умолчанию `:9091`) |
| TLS | mTLS 1.3, наследует `*tls.Config` брокера (`MinVersion = tls.VersionTLS13`) |
| Аутентификация | CN клиентского сертификата должен начинаться с префикса `admin-` |
| Авторизация | gRPC Unary Interceptor (`adminAuthInterceptor`) |
| Версия протокола | Protobuf 3 (`internal/admin/proto/admin.proto`) |
| Полное имя сервиса | `admin.AdminService` |

> [!WARNING]
> **Admin API требует mTLS и строгой валидации CN.** Никогда не запускайте Admin API без `tls.require_client_cert: true`. Префикс `admin-` в CN — единственный механизм авторизации; root-CA Admin API должен быть изолирован от пользовательского CA.

---

## 2. Конфигурация

```yaml
tls:
  require_client_cert: true
  client_ca_file: "/etc/aqueduct/client_ca.pem"

admin:
  enabled: true
  addr: ":9091"
```

Переменные окружения:

- `AQUEDUCT_ADMIN_ENABLED` (`bool`)
- `AQUEDUCT_ADMIN_ADDR` (`string`)

Если `admin.enabled: true`, но `acl.enabled: false` — `cmd/broker/main.go` автоматически создаёт `authz.NewEngine(nil, authz.PermAll)` для движка обновлений, чтобы Admin API мог работать независимо от пользовательского ACL.

---

## 3. Аутентификация

`internal/admin/server.go::adminAuthInterceptor` выполняет строгую проверку:

1. Извлекает `peer.FromContext(ctx)`.
2. Проверяет, что `p.AuthInfo != nil` и имеет тип `credentials.TLSInfo`.
3. Проверяет, что `len(tlsInfo.State.PeerCertificates) > 0`.
4. Извлекает `cn = tlsInfo.State.PeerCertificates[0].Subject.CommonName`.
5. Проверяет `strings.HasPrefix(cn, "admin-")`.

Любая ошибка → `codes.Unauthenticated` или `codes.PermissionDenied` с записью в `slog.Logger.Warn`.

Допустимые CN: `admin-operator`, `admin-john`, `admin-sre-team-1`, и т. п. **Не** используйте префикс в CN обычных клиентов — это даст им доступ к Admin API.

---

## 4. RPC: `SetClientQuota`

Метод: `/admin.AdminService/SetClientQuota`

Обновляет rate и burst token-bucket для конкретного клиента. Lock-Free RCU: новая корзина помещается в `quotas.Manager.bucketsPtr` через `atomic.Pointer.Store`.

### Запрос

```protobuf
message SetClientQuotaRequest {
  string client_id = 1;
  int64 rate = 2;
}
```

| Поле | Тип | Описание |
| :--- | :--- | :--- |
| `client_id` | `string` | CN клиента, для которого применяется квота. Должен совпадать с CN TLS-сертификата. |
| `rate` | `int64` | Новая скорость пополнения (msg/sec). `0` = безлимитно. `>0` = активирует token-bucket. |

> Текущий proto-контракт содержит только `rate`. `burst` поднимается до `rate` (или `1000` если `rate==0`) внутри `quotas.SetRate()` — это поведение не описано в контракте, см. `internal/quotas/bucket.go::SetRate`.

### Ответ

```protobuf
message SetClientQuotaResponse {
  bool success = 1;
}
```

`success = true` означает, что квота успешно обновлена.

### Побочные эффекты

- Метрика `aqueduct_admin_requests_total{method="SetClientQuota"}` инкрементируется.
- `slog.Logger.Info("admin API: SetClientQuota", "caller", cn, "client_id", req.ClientId, "rate", req.Rate)` пишется в лог.

### Ошибки

| gRPC Code | Условие |
| :--- | :--- |
| `Unauthenticated` | Нет peer auth context или отсутствует TLS-сертификат. |
| `PermissionDenied` | CN клиента не начинается с `admin-`. |
| `InvalidArgument` | `client_id == ""`. |

---

## 5. RPC: `UpdateACL`

Метод: `/admin.AdminService/UpdateACL`

Полностью заменяет набор правил ACL через RCU-swap в `authz.Engine.rulesPtr`.

### Запрос

```protobuf
message ACLRule {
  string client_id = 1;
  string topic = 2;
  string permission = 3;  // "publish", "subscribe", "all", "none"
}

message UpdateACLRequest {
  repeated ACLRule rules = 1;
}
```

| Поле | Тип | Описание |
| :--- | :--- | :--- |
| `rules` | `[]ACLRule` | Полный набор правил. **Заменяет** старый набор, не мержится. |

Permission строки приводятся к `authz.Permission` через `strings.ToLower + TrimSpace`:

| Строка | Permission |
| :--- | :--- |
| `"publish"` | `PermPublish` (`0x01`) |
| `"subscribe"` | `PermSubscribe` (`0x02`) |
| `"all"` | `PermPublish | PermSubscribe` |
| `"none"` (или неизвестное) | `PermNone` |

Правила компилируются в `map[uint64]Permission`, где ключ — `authz.CombineHashStrings(client_id, topic)` (некоммутативный FNV-1a).

### Ответ

```protobuf
message UpdateACLResponse {
  bool success = 1;
  int32 rules_count = 2;
}
```

| Поле | Описание |
| :--- | :--- |
| `success` | `true` если обновление выполнено. |
| `rules_count` | Количество правил в новой матрице (int32, после слияния по ключу). |

### Побочные эффекты

- `authz.Engine.Reload(newRules)` → атомарный swap `atomic.Pointer[map[uint64]Permission]`.
- Hot-path проверки в `authz.Allowed` сразу читают новую матрицу без блокировок.
- Метрика `aqueduct_admin_requests_total{method="UpdateACL"}` инкрементируется.
- Лог `slog.Logger.Info("admin API: UpdateACL", "caller", cn, "rules_count", len(rules))`.

### Ошибки

| gRPC Code | Условие |
| :--- | :--- |
| `Unauthenticated` | Нет TLS-сертификата. |
| `PermissionDenied` | CN клиента не начинается с `admin-`. |
| `Unavailable` | `authz.Engine` не инициализирован (`authz == nil`). |

> **NB**: пустой список `rules = []` легитимен — он очищает ACL до `default`-политики.

---

## 6. Идемпотентность

| RPC | Идемпотентный | Побочный эффект |
| :--- | :--- | :--- |
| `SetClientQuota` | Да | Перезаписывает корзину клиента. Повторный вызов с тем же `rate` оставляет квоту неизменной. |
| `UpdateACL` | Да | Полностью заменяет матрицу. Повторный вызов с тем же списком возвращает идентичное состояние. |

Обе RPC безопасны для retry; рекомендуется deadline ≥ 5 секунд.

---

## 7. Метрики Admin API

| Метрика | Метки | Когда |
| :--- | :--- | :--- |
| `aqueduct_admin_requests_total{method="SetClientQuota"}` | `method` | При каждом успешном `SetClientQuota`. |
| `aqueduct_admin_requests_total{method="UpdateACL"}` | `method` | При каждом успешном `UpdateACL`. |

Неудачные вызовы (отвергнутые интерцептором) **не** инкрементируют счётчик. Для подсчёта отказов используйте `slog`-логи (`"admin API access denied: invalid common name"`).

---

## 8. Генерация клиента

Клиент на Go:

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials"
    adminpb "github.com/kshishtovsky/aqueduct/internal/admin/proto"
)

func main() {
    cert, err := tls.LoadX509KeyPair("admin.crt", "admin.key")
    if err != nil { panic(err) }
    pool := x509.NewCertPool()
    pool.AddCert(caCert)
    tlsConf := &tls.Config{
        Certificates: []tls.Certificate{cert},
        RootCAs:      pool,
        ServerName:   "aqueduct-broker",
        MinVersion:   tls.VersionTLS13,
    }
    conn, err := grpc.NewClient(":9091",
        grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)),
    )
    if err != nil { panic(err) }
    defer conn.Close()

    client := adminpb.NewAdminServiceClient(conn)
    resp, err := client.SetClientQuota(ctx, &adminpb.SetClientQuotaRequest{
        ClientId: "service-a",
        Rate:     100,
    })
    if err != nil { panic(err) }
    _ = resp
}
```

`grpc.WithTransportCredentials(credentials.NewTLS(tlsConf))` обеспечивает mTLS на стороне клиента. CN клиентского сертификата **обязан** начинаться с `admin-`.

---

## 9. Генерация proto

Файлы `admin.pb.go` и `admin_grpc.pb.go` сгенерированы из `internal/admin/proto/admin.proto`. Перегенерация:

```bash
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    internal/admin/proto/admin.proto
```

`protoc-gen-go` и `protoc-gen-go-grpc` фиксируются в `tools.go`.

---

## 10. Безопасность

- **CN-allowlist**: префикс `admin-` хардкоден в `internal/admin/server.go:189`. Изменение требует перекомпиляции.
- **mTLS only**: Admin API не имеет опционального TLS — если `tls.require_client_cert: false`, интерцептор всё равно требует TLS, но без верификации клиента. Это небезопасно. Всегда включайте `require_client_cert: true`.
- **Изоляция CA**: используйте отдельный CA-бандл для Admin-клиентов (`tls.client_ca_file`), отличный от пользовательского.
- **Network policy**: ограничьте доступ к `admin.addr` через firewall/NetworkPolicy только для операторских хостов.
- **Audit**: все успешные вызовы логируются через `slog.Info` с `caller`, `client_id` и `rate`/`rules_count`.