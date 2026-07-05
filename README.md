# Real-Time Transaction Analytics

![CI](https://github.com/abbas1123/realtime-transaction-analytics/actions/workflows/ci.yml/badge.svg)

Kafka Streams service that watches a live payment event stream and raises **fraud alerts in real time** —
windowed aggregation per account with velocity and amount thresholds.

Companion service to [payment-transactions-api](https://github.com/abbas1123/payment-transactions-api):
it consumes the exact `TransactionEvent` contract that API publishes to the `transaction-events` topic.

## Topology

```
 transaction-events (JSON)
        │
        ▼
  filter (valid events)
        │
        ▼
  groupBy fromAccountId
        │
        ▼
  1-minute tumbling window ──► aggregate: txCount + totalAmount
        │                          (state store: account-window-stats)
        ▼
  threshold filter ── txCount ≥ 5  OR  total ≥ 1000
        │
        ▼
  fraud-alerts (JSON) ──► @KafkaListener ──► in-memory store ──► GET /api/alerts
```

Both thresholds and the window size are configuration, not code (`analytics.*` properties).

## Run it

```bash
docker compose up -d          # single-node Kafka (KRaft)
mvn spring-boot:run           # starts on :8081
```

Feed it events — either run payment-transactions-api and make transfers, or pipe events manually:

```bash
docker exec -it analytics-kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic transaction-events

>{"transactionId":1,"fromAccountId":100,"toAccountId":101,"amount":600.00,"commission":3.00,"status":"COMPLETED","occurredAt":"2026-07-06T10:00:00Z"}
>{"transactionId":2,"fromAccountId":100,"toAccountId":102,"amount":450.00,"commission":2.25,"status":"COMPLETED","occurredAt":"2026-07-06T10:00:20Z"}
```

Then:

```bash
curl "http://localhost:8081/api/alerts"
# [{"accountId":"100","reason":"AMOUNT","txCount":2,"totalAmount":1050.0,...}]
```

Swagger UI: http://localhost:8081/swagger-ui.html

## Tests

The whole topology is covered with **`TopologyTestDriver`** — deterministic streaming tests with no broker:
velocity alerts, amount alerts, quiet-below-threshold and window isolation. Run with `mvn test`.

## Design notes

- **Event time, not wall clock** — windows are driven by record timestamps, so replaying a backlog
  yields the same alerts as live traffic.
- **Tumbling windows with no grace** keep the demo deterministic; production would add a grace period
  for late events and `suppress()` for exactly-one-alert-per-window semantics.
- **Alerts are emitted the moment a threshold is crossed** (per aggregate update), rather than when the
  window closes — for fraud, latency beats completeness.
- **Keyed by source account**, so all events of one account land in one partition and are processed in order.
- The REST layer reads a bounded in-memory buffer; the natural next step is interactive queries against
  the `account-window-stats` state store or sinking alerts to a database.
