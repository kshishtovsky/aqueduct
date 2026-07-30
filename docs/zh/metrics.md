# 参考: Prometheus 指标 (v1.16.0)

本文档详述 Aqueduct 暴露的所有 Prometheus 指标（19 个原生指标），通过 `internal/metrics/metrics.go` 注册，通过 `/metrics` HTTP 端点抓取（监听于 `metrics_addr`，默认 `:9090`）。

> [!NOTE]
> Diátaxis 分类：**Reference** — 完整指标清单与标签。

---

## 1. 端点

```bash
# Prometheus 抓取目标
http://<broker>:9090/metrics

# 健康检查
http://<broker>:9090/healthz
```

`docker-compose.yml` 将容器端口 `:9090` 映射到宿主机 `:9091`（仅 Prometheus 抓取端口；健康检查保留 `:9090`）。在 Kubernetes 中，`metrics_addr` 由 ConfigMap 配置。

---

## 2. 消息吞吐量

| 指标 | 类型 | 标签 | 说明 |
| :--- | :--- | :--- | :--- |
| `aqueduct_messages_published_total` | Counter | `topic` | 每主题发布消息总数 |
| `aqueduct_messages_delivered_total` | Counter | `topic` | 每主题投递消息总数 |
| `aqueduct_messages_expired_total` | Counter | `topic`, `priority` | TTL 过期丢弃消息总数 |
| `aqueduct_messages_dropped_total` | Counter | `topic`, `policy` | 因慢消费者 Backpressure 丢弃消息总数（按策略区分） |
| `aqueduct_messages_nacked_total` | Counter | `topic` | NACK 消息总数 |
| `aqueduct_messages_dead_lettered_total` | Counter | `topic` | 路由到 DLQ 的消息总数 |
| `aqueduct_messages_rate_limited_total` | Counter | `client` | 因速率限制丢弃的消息总数 |

**示例 PromQL 查询**：

```promql
# 每秒发布速率（按主题 top-10）
topk(10, rate(aqueduct_messages_published_total[5m]))

# DLQ 速率（告警阈值：> 0.1 msg/s）
rate(aqueduct_messages_dead_lettered_total[5m])

# 过期速率（按优先级）
sum by (priority) (rate(aqueduct_messages_expired_total[5m]))
```

---

## 3. 订阅者状态

| 指标 | 类型 | 说明 |
| :--- | :--- | :--- |
| `aqueduct_active_subscribers` | Gauge | 所有主题的活跃订阅者总数 |
| `aqueduct_durable_subscriptions_active` | Gauge | 持久化订阅者总数 |
| `aqueduct_consumer_offset` | Gauge (`consumer`, `topic`) | 每 (consumer, topic) 当前已确认偏移量 |
| `aqueduct_slow_consumers_disconnected_total` | Counter | 因 Backpressure `disconnect` 策略被驱逐的慢消费者总数 |

---

## 4. 帧解析性能

| 指标 | 类型 | 说明 |
| :--- | :--- | :--- |
| `aqueduct_frame_parse_duration_ns` | Histogram | **当前未激活**：指标已注册，但 v1.16.0 没有调用 `Observe`，因此不会产生有效样本。 |

`aqueduct_frame_parse_duration_ns` 在 v1.16.0 中仅注册、未更新。启用 `Observe` 之前，不要基于该指标创建延迟告警或仪表盘。

---

## 5. 集群网格 (P2P Federation)

| 指标 | 类型 | 说明 |
| :--- | :--- | :--- |
| `aqueduct_cluster_peers_active` | Gauge | 当前活跃对等连接数（含 reconnect 中） |
| `aqueduct_cluster_frames_forwarded_total` | Counter | 转发到对等节点的帧总数 |
| `aqueduct_cluster_frames_received_total` | Counter | 从对等节点接收的 mesh 转发帧总数 |

**示例 PromQL 查询**：

```promql
# 转发吞吐（msg/s）
rate(aqueduct_cluster_frames_forwarded_total[5m])

# 不健康网格（无活跃对等）
aqueduct_cluster_peers_active == 0
```

---

## 6. 加密追加日志 (AAL)

| 指标 | 类型 | 说明 |
| :--- | :--- | :--- |
| `aqueduct_aal_replay_duration_seconds` | Gauge | 启动时 AAL 重放总耗时（秒） |
| `aqueduct_aal_rotations_total` | Counter | **当前未激活**：指标已注册，但生产代码不会调用 `aal.Log.Rotate`，计数器也不会递增。 |
| `aqueduct_aal_backfill_frames_total` | Counter | 订阅者回填期间重放的历史帧总数 |

---

## 7. 授权与速率限制

| 指标 | 类型 | 标签 | 说明 |
| :--- | :--- | :--- | :--- |
| `aqueduct_authz_denied_total` | Counter | `client`, `topic` | ACL 拒绝次数（每客户端每主题） |
| `aqueduct_admin_requests_total` | Counter | `method` | Admin API 请求总数（`SetClientQuota`、`UpdateACL`） |

**示例 PromQL 查询**：

```promql
# 拒绝热点（top-5 客户端）
topk(5, rate(aqueduct_authz_denied_total[5m]))

# Admin API 使用
sum by (method) (rate(aqueduct_admin_requests_total[5m]))
```

---

## 8. 分布式追踪

| 指标 | 类型 | 说明 |
| :--- | :--- | :--- |
| `aqueduct_tracing_spans_total` | Counter | 创建的追踪 Span 总数（仅 `tracing.enabled: true` 时递增） |

---

## 9. Grafana 仪表盘

`deploy/grafana/dashboards/` 提供开箱即用的仪表盘 JSON（与 `docker-compose.yml` 一起加载）。

**推荐的 Grafana 面板**：

| 面板 | PromQL |
| :--- | :--- |
| 消息吞吐（每秒） | `sum(rate(aqueduct_messages_published_total[5m]))` |
| 投递 vs 发布 | `sum(rate(aqueduct_messages_delivered_total[5m])) / sum(rate(aqueduct_messages_published_total[5m]))` |
| 活跃订阅者 | `aqueduct_active_subscribers` |
| 集群对等节点 | `aqueduct_cluster_peers_active` |
| DLQ 速率 | `rate(aqueduct_messages_dead_lettered_total[5m])` |
| 帧解析 P99 | `histogram_quantile(0.99, rate(aqueduct_frame_parse_duration_ns_bucket[5m]))` |
| 速率限制丢弃 | `sum(rate(aqueduct_messages_rate_limited_total[5m]))` |
| ACL 拒绝 | `sum by (client) (rate(aqueduct_authz_denied_total[5m]))` |

---

## 10. 告警规则建议

```yaml
groups:
- name: aqueduct
  rules:
  - alert: AqueductHighDLQ
    expr: rate(aqueduct_messages_dead_lettered_total[5m]) > 0.1
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Aqueduct DLQ rate exceeds 0.1 msg/s"

  - alert: AqueductNoPeers
    expr: aqueduct_cluster_peers_active == 0
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "No active cluster peers"

  - alert: AqueductFrameParseSlow
    expr: histogram_quantile(0.99, rate(aqueduct_frame_parse_duration_ns_bucket[5m])) > 10000
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Frame parse P99 exceeds 10 µs"

  - alert: AqueductHighAuthzDenials
    expr: sum(rate(aqueduct_authz_denied_total[5m])) > 10
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "ACL denial rate exceeds 10/s"

  - alert: AqueductSlowConsumers
    expr: rate(aqueduct_slow_consumers_disconnected_total[5m]) > 0
    for: 5m
    labels:
      severity: info
    annotations:
      summary: "Slow consumers being disconnected"
```

---

## 11. 指标标签基数

> [!WARNING]
> `topic` 标签可能具有**高基数**（每主题一个时间序列）。监控生产部署中的活跃主题数量。若超过 10k 活跃主题，考虑：
> - 通过 Prometheus 联邦将高基数标签在边缘移除。
> - 使用 `metric_relabel_configs` 在抓取时丢弃高频 topic 标签。
> - 在 `config.yaml` 中限定允许的主题命名空间。