package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:4242", "Aqueduct broker UDP address")
	concurrency := flag.Int("c", 10, "Number of concurrent connections/streams")
	totalReqs := flag.Int("n", 100000, "Total number of requests/messages to send")
	payloadSize := flag.Int("size", 128, "Message payload size in bytes")
	topic := flag.String("topic", "bench", "Topic name for publish benchmark")
	timeout := flag.Duration("timeout", 5*time.Second, "Request deadline per message")
	batchSize := flag.Int("batch", 1, "Batch size (1 = single frames)")
	flag.Parse()

	if *concurrency <= 0 || *totalReqs <= 0 || *payloadSize <= 0 || *batchSize <= 0 {
		log.Fatal("Invalid parameters: -c, -n, -size, and -batch must be > 0")
	}

	fmt.Println("=========================================================")
	fmt.Println(" Aqueduct High-Performance Load Testing Tool (aqueduct-bench)")
	fmt.Println("=========================================================")
	fmt.Printf(" Target Broker      : %s\n", *addr)
	fmt.Printf(" Concurrency        : %d workers\n", *concurrency)
	fmt.Printf(" Total Requests     : %d messages\n", *totalReqs)
	fmt.Printf(" Payload Size       : %d bytes\n", *payloadSize)
	fmt.Printf(" Batch Size         : %d\n", *batchSize)
	fmt.Printf(" Topic              : %s\n", *topic)
	fmt.Println("---------------------------------------------------------")

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"aqueduct-v1"},
		MinVersion:         tls.VersionTLS13,
	}

	payload := make([]byte, *payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	reqsPerWorker := *totalReqs / *concurrency
	remainder := *totalReqs % *concurrency

	var (
		completedReqs atomic.Int64
		failedReqs    atomic.Int64
		wg            sync.WaitGroup
		mu            sync.Mutex
		allLatencies  []time.Duration
	)

	startTime := time.Now()
	wg.Add(*concurrency)

	for w := 0; w < *concurrency; w++ {
		count := reqsPerWorker
		if w == 0 {
			count += remainder
		}

		go func(workerID int, numReqs int) {
			defer wg.Done()

			conn, err := quic.DialAddr(
				context.Background(),
				*addr,
				tlsConf,
				&quic.Config{MaxIdleTimeout: 30 * time.Second},
			)
			if err != nil {
				log.Printf("[Worker %d] Failed to connect: %v\n", workerID, err)
				failedReqs.Add(int64(numReqs))
				return
			}
			defer func() { _ = conn.CloseWithError(0, "bench worker done") }()

			stream, err := conn.OpenStreamSync(context.Background())
			if err != nil {
				log.Printf("[Worker %d] Failed to open stream: %v\n", workerID, err)
				failedReqs.Add(int64(numReqs))
				return
			}
			defer stream.Close()

			workerLatencies := make([]time.Duration, 0, numReqs)

			if *batchSize <= 1 {
				for i := 0; i < numReqs; i++ {
					reqStart := time.Now()
					_ = stream.SetWriteDeadline(time.Now().Add(*timeout))

					buf := protocol.SerializeFrame(protocol.CmdPublish, uint32(i+1), payload)
					_, err := stream.Write(*buf)
					protocol.ReleaseBuffer(buf)

					if err != nil {
						failedReqs.Add(1)
						continue
					}

					duration := time.Since(reqStart)
					workerLatencies = append(workerLatencies, duration)
					completedReqs.Add(1)
				}
			} else {
				frameTotalSize := protocol.HeaderSize + *payloadSize
				maxBatchPayloadSize := *batchSize * frameTotalSize
				batchPayloadBuf := make([]byte, 0, maxBatchPayloadSize)
				msgTimestamps := make([]time.Time, *batchSize)
				msgCount := 0

				flush := func() {
					_ = stream.SetWriteDeadline(time.Now().Add(*timeout))

					buf := protocol.SerializeFrame(protocol.CmdPublishBatch, 0, batchPayloadBuf)
					_, err := stream.Write(*buf)
					protocol.ReleaseBuffer(buf)

					if err != nil {
						failedReqs.Add(int64(msgCount))
					} else {
						now := time.Now()
						for j := 0; j < msgCount; j++ {
							workerLatencies = append(workerLatencies, now.Sub(msgTimestamps[j]))
						}
						completedReqs.Add(int64(msgCount))
					}

					batchPayloadBuf = batchPayloadBuf[:0]
					msgCount = 0
				}

				for i := 0; i < numReqs; i++ {
					reqStart := time.Now()
					msgTimestamps[msgCount] = reqStart

					offset := len(batchPayloadBuf)
					batchPayloadBuf = batchPayloadBuf[:offset+frameTotalSize]
					batchPayloadBuf[offset] = protocol.MagicByte
					batchPayloadBuf[offset+1] = byte(protocol.CmdPublish)
					binary.LittleEndian.PutUint32(batchPayloadBuf[offset+2:offset+6], uint32(i+1))
					binary.LittleEndian.PutUint32(batchPayloadBuf[offset+6:offset+10], uint32(*payloadSize))
					copy(batchPayloadBuf[offset+10:], payload)
					msgCount++

					if msgCount == *batchSize {
						flush()
					}
				}

				if msgCount > 0 {
					flush()
				}
			}

			mu.Lock()
			allLatencies = append(allLatencies, workerLatencies...)
			mu.Unlock()
		}(w, count)
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	printReport(completedReqs.Load(), failedReqs.Load(), totalTime, *payloadSize, *batchSize, allLatencies)
}

func printReport(completed, failed int64, totalTime time.Duration, payloadSize, batchSize int, latencies []time.Duration) {
	fmt.Println(" Benchmark Results")
	fmt.Println("---------------------------------------------------------")
	fmt.Printf(" Total Time Taken   : %v\n", totalTime)
	fmt.Printf(" Successful Requests: %d\n", completed)
	fmt.Printf(" Failed Requests    : %d\n", failed)

	if completed == 0 {
		fmt.Println("\nERROR: No requests completed successfully.")
		os.Exit(1)
	}

	rps := float64(completed) / totalTime.Seconds()
	mbPerSec := (float64(completed*int64(protocol.HeaderSize+payloadSize)) / (1024 * 1024)) / totalTime.Seconds()

	fmt.Printf(" Batch Size         : %d\n", batchSize)
	fmt.Printf(" Messages / Sec     : %.2f msg/s\n", rps)
	fmt.Printf(" Throughput         : %.2f MB/s\n", mbPerSec)

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	mean := sum / time.Duration(len(latencies))

	minLat := latencies[0]
	maxLat := latencies[len(latencies)-1]

	p50 := getPercentile(latencies, 50.0)
	p90 := getPercentile(latencies, 90.0)
	p99 := getPercentile(latencies, 99.0)
	p999 := getPercentile(latencies, 99.9)

	fmt.Println("\n Latency Breakdown:")
	fmt.Printf("   Min              : %v\n", minLat)
	fmt.Printf("   Mean             : %v\n", mean)
	fmt.Printf("   Max              : %v\n", maxLat)
	fmt.Printf("   p50 (Median)     : %v\n", p50)
	fmt.Printf("   p90              : %v\n", p90)
	fmt.Printf("   p99              : %v\n", p99)
	fmt.Printf("   p99.9            : %v\n", p999)
	fmt.Println("=========================================================")
}

func getPercentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil((p / 100.0) * float64(len(sorted))))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}
