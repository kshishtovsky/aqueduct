# Руководство: Cluster Mesh и TLS (v1.16.0)

Полное руководство по развёртыванию и защите прямой mesh-кластеризации Aqueduct.

> **Diátaxis:** Практическое руководство (How-to) — конкретные шаги по настройке TLS, DNS discovery и сертификатов.

---

## 1. Архитектура

Aqueduct использует **Direct Mesh Clustering** без консенсуса (нет Raft/Paxos). Узлы соединяются через outbound QUIC-стримы и пересылают сообщения по принципу fire-and-forget.

```text
   ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
   │  aqueduct-0  │ ◄─QUIC─►│  aqueduct-1  │ ◄─QUIC─►│  aqueduct-2  │
   │  :4242       │         │  :4242       │         │  :4242       │
   └──────────────┘         └──────────────┘         └──────────────┘
         ▲                        ▲                        ▲
         │ client                 │ client                 │ client
         ▼ QUIC bidi              ▼ QUIC bidi              ▼ QUIC bidi
   ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
   │  clients     │         │  clients     │         │  clients     │
   └──────────────┘         └──────────────┘         └──────────────┘
```

Клиенты подключаются к **любому** узлу; сообщения пересылаются между узлами через mesh.

---

## 2. Статические пиры (Manual Federation)

### 2.1 Конфигурация

```yaml
cluster:
  peers:
    - "aqueduct-1.example.com:4242"
    - "aqueduct-2.example.com:4242"
  discovery:
    enabled: false
```

Каждый узел перечисляет **другие** узлы. В N-узловом кластере каждый конфиг содержит N-1 адресов.

### 2.2 Как работает `cluster.PeerManager`

`internal/cluster/cluster.go::PeerManager`:

1. На старте `NewWithLogger` создаёт `PeerRef` для каждого адреса и запускает `reconnectLoop` в отдельной горутине.
2. Каждый `reconnectLoop` вызывает `quic.DialAddr` с экспоненциальным backoff (`initialBackoff = 250 ms`, `maxBackoff = 30 s`).
3. После успешного dial открывается bidi-stream через `conn.OpenStreamSync(ctx)`.
4. `stream.Read(buf)` блокирует до обрыва соединения — **peer-stream only writes** (mesh-pipe).
5. При обрыве: stream = nil, цикл повторяет dial с backoff.

Управление пирами через RCU-паттерн (`atomic.Pointer[peerSlice]`):

- `Forward(rawBuf, addForwardedBit=true)` — hot path, **ноль блокировок**, читает `peers.Load()`.
- `AddPeer(ctx, addr)` / `RemovePeer(addr)` — мутация с `pm.mu`, копия слайса, атомарный swap.

### 2.3 MeshForwarded Bit

При пересылке `PeerManager.Forward` устанавливает **Бит 7 Command-байта** (`0x80`, `MeshForwardedBit`) в общем буфере **на месте**:

```go
orig := rawBuf[1]
rawBuf[1] = orig | byte(protocol.MeshForwardedBit)
_, werr := s.Write(rawBuf)
rawBuf[1] = orig
```

Получающий пир проверяет `protocol.IsForwarded(cmd)` и диспатчит фрейм в `Router.PublishFromPeer` **без повторной пересылки**. Это предотвращает петлевые штормы в многоузловой топологии.

Zero-copy forwarding: буфер из `sync.Pool` отправляется пиру как есть; **никакой аллокации** в общем случае.

---

## 3. DNS Discovery (Kubernetes Headless Service)

### 3.1 Конфигурация

```yaml
cluster:
  peers: []  # пусто — discovery заполняет автоматически
  discovery:
    enabled: true
    type: "dns"
    host: "aqueduct-headless.default.svc.cluster.local"
    port: "4242"
    interval: "10s"
```

### 3.2 Headless Service (обязательно)

```yaml
apiVersion: v1
kind: Service
metadata:
  name: aqueduct-headless
spec:
  clusterIP: None
  ports:
    - name: quic
      port: 4242
  selector:
    app.kubernetes.io/name: aqueduct
```

`clusterIP: None` → DNS возвращает A-записи для каждого готового пода StatefulSet.

### 3.3 Алгоритм

`internal/cluster/discovery.go::Discovery.resolve`:

1. `net.LookupHost(hostname)` → `[]string` IP-адресов.
2. `normalize()` дедуплицирует и фильтрует через `net.ParseIP`.
3. Diff с `knownIPs`:
   - Новые IP → `PeerManager.AddPeer(ctx, addr)`.
   - Исчезнувшие IP → `PeerManager.RemovePeer(addr)`.
4. `knownIPs = normalized`.

Polling каждые `interval` (по умолчанию 10s).

### 3.4 Почему DNS, а не client-go

| Аспект | DNS | client-go |
| :--- | :--- | :--- |
| Размер бинарника | 0 MB (stdlib `net`) | ~40 MB |
| Зависимости | Нет | REST + protobuf + informers |
| Динамические обновления | Headless Service → A records → `LookupHost` | Watch + label selector |
| Single static binary | Да | Нет |

---

## 4. Mesh TLS

### 4.1 ALPN для mesh

`cmd/broker/main.go:162` устанавливает `NextProtos: ["aqueduct-mesh"]` для peer-соединений (отличается от клиентского `aqueduct-v1`). Это позволяет GOFER-инспекторам различать mesh и клиентский трафик.

### 4.2 Параметры

```yaml
cluster:
  mesh:
    insecure_skip_verify: false
    ca_file: "/etc/aqueduct/mesh_ca.pem"
```

| Поле | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `cluster.mesh.insecure_skip_verify` | `bool` | `false` | Отключает верификацию TLS-сертификата пира. **Запрещено в production.** |
| `cluster.mesh.ca_file` | `string` | `""` | PEM-бандл CA для подписи mesh-сертификатов. Пусто → системный пул. |

### 4.3 Режимы

| `insecure_skip_verify` | `ca_file` | Поведение |
| :--- | :--- | :--- |
| `false` | `""` | Использует системный CA pool (`x509.SystemCertPool`). Подходит, если mesh-CA в `ca-certificates`. |
| `false` | `"/path/to/ca.pem"` | Использует указанный CA-бандл. **Рекомендуется.** |
| `true` | `""` | TLS шифрует, но не верифицирует identity пира. Уязвимо к MITM. |
| `true` | `"/path/to/ca.pem"` | То же, что и выше — флаг `ca_file` игнорируется. |

> [!WARNING]
> **`cluster.mesh.insecure_skip_verify: true` отключает проверку TLS-сертификата пира** в `cmd/broker/main.go::main`. Это оставляет mesh **открытым к MITM-атакам**: любой сетевой узел между двумя брокерами может перехватить соединение, представившись mesh-узлом. В production:
>
> 1. Установите `insecure_skip_verify: false`.
> 2. Разверните выделенный **internal Cluster CA**.
> 3. Подпишите mesh-сертификаты (`tls.cert_file` / `tls.key_file`) этим CA.
> 4. Пропишите `cluster.mesh.ca_file` для всех узлов.
>
> Использование `insecure_skip_verify: true` оправдано только в dev/test mesh с самоподписанными сертификатами.

### 4.4 Генерация Cluster CA и сертификатов

Пример с `cfssl` (или `openssl`):

```bash
# 1. Cluster CA
openssl genrsa -out mesh_ca.key 2048
openssl req -new -x509 -days 3650 -key mesh_ca.key -out mesh_ca.pem \
    -subj "/CN=Aqueduct Mesh CA/O=Acme Corp"

# 2. Server key + CSR (для каждого узла)
openssl genrsa -out aqueduct-0.key 2048
openssl req -new -key aqueduct-0.key -out aqueduct-0.csr \
    -subj "/CN=aqueduct-0/O=Aqueduct Mesh"

# 3. Подпись сертификата Cluster CA
openssl x509 -req -in aqueduct-0.csr -CA mesh_ca.pem -CAkey mesh_ca.key \
    -CAcreateserial -out aqueduct-0.crt -days 365 \
    -extfile <(printf "subjectAltName=DNS:aqueduct-0,DNS:aqueduct-0.aqueduct-headless.default.svc.cluster.local,IP:10.0.0.1")
```

SAN сертификата должен включать DNS/IP, по которым mesh-узлы соединяются друг с другом.

### 4.5 Конфигурация на стороне брокера

```yaml
tls:
  generate: false
  cert_file: "/etc/aqueduct/aqueduct-0.crt"
  key_file: "/etc/aqueduct/aqueduct-0.key"

cluster:
  mesh:
    insecure_skip_verify: false
    ca_file: "/etc/aqueduct/mesh_ca.pem"
  peers:
    - "aqueduct-1:4242"
    - "aqueduct-2:4242"
```

> Сертификат узла **общий** для клиентского и mesh-трафика — это упрощает ротацию. SAN должен покрывать оба варианта использования.

---

## 5. Производительность mesh

| Метрика | Значение | Где |
| :--- | :--- | :--- |
| Forward latency (3-node mesh, in-VM) | ~3 µs | `cluster.PeerManager.Forward` |
| Аллокаций на Forward | 0 | `Forward` mutates buf[1] in-place |
| Reconnect backoff | 250 ms → 30 s | `cluster.waitBackoffOrDone` |
| DNS resolve interval | 10 s (default) | `cluster.Discovery.Start` |

---

## 6. Failure modes

### 6.1 Узел недоступен

`reconnectLoop` диалит с экспоненциальным backoff. При `pm.closed.Load() == true` → выход. Подключённые стримы обрываются (`stream.Read` → EOF), цикл повторяет dial.

### 6.2 DNS resolve failed

`d.resolver.LookupHost(ctx, hostname)` ошибка → `slog.Warn`, **никаких изменений в knownIPs**. Уже подключённые пиры остаются в mesh.

### 6.3 Forwarding failure

`stream.Write` ошибка → пир пропускается (continue), инкрементируется `aqueduct_cluster_frames_forwarded_total` (только успешные). Неудачные forward'ы не влияют на локальную доставку.

### 6.4 Узел удалён из DNS (Scale down)

`Discovery.resolve` находит, что IP исчез → `pm.RemovePeer(addr)` → `cancel()`, закрытие stream, RCU-swap без удалённого `PeerRef`.

---

## 7. Масштабирование

### 7.1 Helm

```bash
helm upgrade aqueduct ./deploy/helm/aqueduct \
    --set replicaCount=5
```

После upgrade:

1. K8s создаёт `aqueduct-3`, `aqueduct-4` StatefulSet поды.
2. Headless Service автоматически публикует новые A-записи.
3. Существующие поды опрашивают DNS (через `interval=10s`) → обнаруживают новые пиры → `AddPeer`.
4. Новые поды при старте опрашивают DNS → обнаруживают все остальные поды → подключаются.

Convergence time: ≤ `interval` + backoff.

### 7.2 Raw manifests

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

### 7.3 Лимиты

- **Fire-and-forget**: гарантии порядка между узлами **нет**.
- **Mesh size**: рекомендуется ≤ 16 узлов. Большие mesh'и увеличивают latency fan-out и нагрузку на CPU.
- **Подписчики**: scaling-out за счёт **добавления реплик брокера**, не увеличения размера mesh.

---

## 8. End-to-end check-list для production

- [ ] Каждый узел имеет собственный TLS-сертификат, выданный internal Cluster CA.
- [ ] SAN сертификата содержит FQDN mesh-узла (для DNS-обнаружения).
- [ ] `cluster.mesh.insecure_skip_verify: false`.
- [ ] `cluster.mesh.ca_file` указывает на bundle Cluster CA.
- [ ] Headless Service развёрнут в том же namespace, что и StatefulSet.
- [ ] `cluster.discovery.enabled: true`.
- [ ] DNS polling interval ≥ 5s (default 10s).
- [ ] Мониторинг `aqueduct_cluster_peers_active` на каждом узле.
- [ ] Алерт на `aqueduct_cluster_frames_forwarded_total` — нулевой rate означает потерю mesh.