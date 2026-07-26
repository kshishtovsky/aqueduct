# Туториал: Быстрый старт с Aqueduct

Это руководство поможет вам запустить брокер сообщений Aqueduct QUIC, оформить подписку на топик и опубликовать бинарное сообщение.

## Требования

Перед началом работы убедитесь, что у вас установлены:
- **Go 1.22+**
- Терминальное окружение (Linux или macOS)

## Шаг 1: Клонирование и сборка брокера

Склонируйте репозиторий и скомпилируйте исполняемый файл:

```bash
git clone https://github.com/kshishtovsky/aqueduct.git
cd aqueduct
go build -o bin/broker ./cmd/broker
```

## Шаг 2: Запуск брокера в режиме разработки

Запустите скомпилированный файл брокера:

```bash
./bin/broker -addr :4242 -metrics-addr :9090
```

В консоли отобразится следующий вывод:

```text
2026/07/26 04:42:00 WARN Using ephemeral self-signed certificate. Do not use in production.
2026/07/26 04:42:00 INFO metrics server started addr=:9090
2026/07/26 04:42:00 INFO broker listening addr=127.0.0.1:4242
2026/07/26 04:42:00 INFO broker started addr=127.0.0.1:4242
```

## Шаг 3: Проверка Health Check

Откройте второе окно терминала и проверьте доступность HTTP эндпоинта:

```bash
curl http://localhost:9090/healthz
# Вывод: OK
```

## Шаг 4: Отправка и получение сообщений на Go

Создайте файл `example_client.go` для подключения к брокеру через QUIC и обмена сообщениями:

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

func main() {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true, // Для dev self-signed сертификата
		NextProtos:         []string{"aqueduct-v1"},
		MinVersion:         tls.VersionTLS13,
	}

	conn, err := quic.DialAddr(context.Background(), "127.0.0.1:4242", tlsConf, nil)
	if err != nil {
		log.Fatalf("ошибка подключения: %v", err)
	}

	// 1. Открытие стрима и подписка
	subStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("ошибка открытия стрима: %v", err)
	}

	subPayload := []byte("topic:orders")
	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 1, subPayload)
	_, _ = subStream.Write(*subBuf)
	protocol.ReleaseBuffer(subBuf)

	fmt.Println("Подписка на топик 'orders' оформлена. Ожидание сообщений...")

	// 2. Открытие стрима и публикация
	pubStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("ошибка открытия стрима публикации: %v", err)
	}

	pubPayload := []byte("orders")
	pubBuf := protocol.SerializeFrame(protocol.CmdPublish, 2, pubPayload)
	_, _ = pubStream.Write(*pubBuf)
	protocol.ReleaseBuffer(pubBuf)

	// 3. Чтение доставленного сообщения подписчиком
	readBuf := make([]byte, 1024)
	n, err := subStream.Read(readBuf)
	if err != nil {
		log.Fatalf("ошибка чтения сообщения: %v", err)
	}

	frame, err := protocol.ParseFrame(readBuf[:n])
	if err != nil {
		log.Fatalf("ошибка парсинга фрейма: %v", err)
	}

	fmt.Printf("Получено доставленное сообщение в топике '%s'\n", string(frame.Payload))
}
```

## Следующие шаги

Переходите к [Руководству по Production развертыванию](production-deployment.md) для настройки TLS сертификатов, лога AAL и мониторинга Prometheus.
