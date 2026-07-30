# Reference: Prometheus Metrics (v1.16.0)

This document is the authoritative inventory of every Prometheus metric emitted by the Aqueduct broker. It is **information-oriented**: lookup a metric name to find its type, labels, and source. For how metrics fit into the production deployment story see [Production Deployment §9](production-deployment.md); for the architectural background see [Architecture & Memory Model](architecture.md).

The metrics HTTP server (`internal/metrics.StartServer(addr)`) exposes:

- `GET /metrics` — Prometheus text format.
- `GET /healthz` — `200 OK` (liveness/readiness probe target).

`metrics_addr` defaults to `:9090`. The server is hardened with `ReadHeaderTimeout: 5s` to prevent Slowloris attacks.

> **Emission model.** All metrics are declared in `internal/metrics/metrics.go` and registered via `prometheus.MustRegister` at startup. Two mechanisms update them:
>
> 1. **Callback-based metrics** — the Router accepts a `RouterMetrics` interface (`internal/broker/router.go:83-88`) and calls `OnPublish`, `OnDeliver`, `SetActiveSubscribers`, `OnRateLimited` from its hot paths. The production implementation is `prometheusMetrics` in `cmd/broker/main.go:397-413`, which is the only bridge that increments `MessagesPublished`, `MessagesDelivered`, `ActiveSubscribers`, and `MessagesRateLimited`. Tests inject mock implementations.
> 2. **Direct increment** — `MessagesExpired`, `MessagesDropped`, `MessagesNacked`, `MessagesDeadLettered`, `SlowConsumersDisconnected`, `AALBackfillFrames`, `DurableSubscribersActive`, `ConsumerOffset`, `ClusterFramesReceived` (`internal/broker/router.go`); `ClusterPeersActive` and `ClusterFramesForwarded` (`internal/cluster/cluster.go`); `AALReplayDuration`, `AuthzDenied`, `TracingSpansTotal` (`internal/transport/broker.go`); `AdminRequestsTotal` (`internal/admin/server.go`).
>
> Metrics marked **Inactive** in the tables below are registered but never incremented in v1.16.0 — they always return `0` and should not be used for alerting.

---

## 1. Message Throughput

| Metric | Type | Labels | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| `aqueduct_messages_published_total` | Counter | `topic` | `Router.publishWithClientID`, `Router.publishLocal` (via `RouterMetrics.OnPublish` callback → `prometheusMetrics.OnPublish` in `cmd/broker/main.go`) | Frames published locally (post-rate-limit). |
| `aqueduct_messages_delivered_total` | Counter | `topic` | `Router.runSubscriberWriter` (via `RouterMetrics.OnDeliver` callback → `prometheusMetrics.OnDeliver` in `cmd/broker/main.go`) | Frames written to a subscriber's QUIC stream. |
| `aqueduct_messages_expired_total` | Counter | `topic, priority` | `Router.runSubscriberWriter` | Frames dropped because `priority_ttls[priority]` elapsed before dequeue. |
| `aqueduct_messages_dropped_total` | Counter | `topic, policy` | `Router.handleOverflow` | Frames dropped by the per-priority queue's backpressure policy. |
| `aqueduct_messages_rate_limited_total` | Counter | `client` | `Router.publishWithClientID` (via `RouterMetrics.OnRateLimited` callback → `prometheusMetrics.OnRateLimited` in `cmd/broker/main.go`). The `quotas.Manager.TryAcquire` call returns a bool; the metric is incremented by the Router through the callback after `TryAcquire` returns `false`. | Publish attempts rejected by the token bucket. |
| `aqueduct_messages_nacked_total` | Counter | `topic` | `Router.runSubscriberWriter.handleNack` | Negative acknowledgements received. |
| `aqueduct_messages_dead_lettered_total` | Counter | `topic` | `Router.runSubscriberWriter.handleNack` | Frames routed to `__dlq__<topic>` after `max_retries`. |

---

## 2. Subscribers & Consumer State

| Metric | Type | Labels | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| `aqueduct_active_subscribers` | Gauge | — | `Router.Subscribe`, `Router.disconnectSubscriber` (via `RouterMetrics.SetActiveSubscribers` callback → `prometheusMetrics.SetActiveSubscribers` in `cmd/broker/main.go`) | Live subscriber count across all topics. |
| `aqueduct_durable_subscriptions_active` | Gauge | — | `Router.Subscribe` | Durable subscribers currently registered. |
| `aqueduct_consumer_offset` | Gauge | `consumer, topic` | `Router.Subscribe` (durable / group ack paths) | Last acknowledged offset for `(consumer_id, topic)`. |

---

## 3. Authorization & ACL

| Metric | Type | Labels | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| `aqueduct_authz_denied_total` | Counter | `client, topic` | `Broker.prepareFrame` | Frames rejected by `authz.Engine.Allowed`. |

---

## 4. Slow Consumer Isolation

| Metric | Type | Labels | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| `aqueduct_slow_consumers_disconnected_total` | Counter | — | `Router.disconnectSubscriber` | Subscribers torn down because the queue overflowed under `disconnect` policy. |

---

## 5. Append-Only Log (AAL)

| Metric | Type | Labels | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| `aqueduct_aal_replay_duration_seconds` | Gauge | — | `Broker.ReplayAAL` | Wall-clock duration of the startup replay. |
| `aqueduct_aal_rotations_total` | Counter | — | **Inactive.** Registered via `prometheus.MustRegister` but **never `.Inc()`-ed** in v1.16.0 (`internal/metrics/metrics.go:77-82,177`). `aal.Log.Rotate(...)` is implemented but no scheduler calls it. The counter always returns `0`. Do not alert on this metric. |
| `aqueduct_aal_backfill_frames_total` | Counter | — | `Router.runBackfillWorker` | Historical AAL frames replayed into a reconnecting durable subscriber. |

---

## 6. Cluster Mesh

| Metric | Type | Labels | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| `aqueduct_cluster_peers_active` | Gauge | — | `PeerManager.runPeerStream` | Number of peers with an active QUIC stream. |
| `aqueduct_cluster_frames_forwarded_total` | Counter | — | `PeerManager.Forward` | Frames broadcast to peers. |
| `aqueduct_cluster_frames_received_total` | Counter | — | `Router.PublishFromPeer` (`internal/broker/router.go:1066`). Incremented at the start of `PublishFromPeer` (called from `Broker.handleForwardedFrame` for `CmdPublish`); batched peer frames (`CmdPublishBatch`) reach `PublishBatch` directly and do **not** increment this counter. |

---

## 7. Frame Parser

| Metric | Type | Labels | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| `aqueduct_frame_parse_duration_ns` | Histogram (`prometheus.DefBuckets`) | — | **Inactive.** Registered via `prometheus.MustRegister` but **never `.Observe()`-d** in v1.16.0 (`internal/metrics/metrics.go:31-37,171`). All buckets return `0`. Do not use this metric as a source of parser p99 latency — `histogram_quantile(...)` will always yield `NaN`. |

---

## 8. Distributed Tracing

| Metric | Type | Labels | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| `aqueduct_tracing_spans_total` | Counter | — | `Broker.startPublishSpan` | OTel spans created on the publish path. |

Useful for confirming that `tracing.enabled: true` actually wires the tracer. Disabled tracing produces no spans but the metric counter is also never incremented.

---

## 9. Admin API

| Metric | Type | Labels | Source | Description |
| :--- | :--- | :--- | :--- | :--- |
| `aqueduct_admin_requests_total` | Counter | `method` | `admin.Server.SetClientQuota`, `admin.Server.UpdateACL` | gRPC RPCs received. Method labels are `SetClientQuota` and `UpdateACL`. |

A spike in this counter with no `SetClientQuota` / `UpdateACL` changes is suspicious — review the audit log for unauthorized role attempts (`PermissionDenied` log lines from `adminAuthInterceptor`).

---

## 10. Scrape Configuration

The bundled `prometheus.yml` scrapes `:9090/metrics`:

```yaml
scrape_configs:
  - job_name: 'aqueduct'
    static_configs:
      - targets: ['aqueduct:9090']
    metrics_path: /metrics
    scrape_interval: 15s
```

For Kubernetes, use the standard `prometheus.io/scrape` annotations on the broker pod and an in-cluster Prometheus operator.

---

## 11. Recommended Alerts

A minimal starter set of Prometheus alerting rules:

```yaml
groups:
  - name: aqueduct
    rules:
      - alert: AqueductBrokerDown
        expr: up{job="aqueduct"} == 0
        for: 1m
        labels:
          severity: critical

      - alert: AqueductHighRateLimited
        expr: |
          rate(aqueduct_messages_rate_limited_total[5m]) > 50
        for: 5m
        labels:
          severity: warning

      - alert: AqueductDeadLetteringSpike
        expr: |
          rate(aqueduct_messages_dead_lettered_total[5m]) > 1
        for: 5m
        labels:
          severity: warning

      - alert: AqueductMeshPeersLost
        expr: |
          aqueduct_cluster_peers_active < 1
        for: 30s
        labels:
          severity: warning
```

Adapt thresholds to your traffic profile. Tune `for:` to suppress flapping on brief outages.