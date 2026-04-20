# LoomQ Deployment Guide

Version: `0.7.0-SNAPSHOT`

This guide matches the current `Intent`-based standalone server.

## Quick Start

### Prerequisites

- JDK 25+
- Maven 3.9+
- Optional: Docker 20.10+ for containerized deployment

### Build

```bash
git clone https://github.com/yourusername/loomq.git
cd loomq
mvn clean package -DskipTests
```

### Run

```bash
java -jar loomq-server/target/loomq-server-0.7.0-SNAPSHOT.jar
```

The server listens on `http://localhost:8080` by default.

### Verify

```bash
curl http://localhost:8080/health
curl http://localhost:8080/health/ready
curl http://localhost:8080/metrics
```

Create an `Intent`:

```bash
curl -X POST http://localhost:8080/v1/intents \
  -H "Content-Type: application/json" \
  -d '{
    "executeAt": "2026-04-20T10:00:00Z",
    "deadline": "2026-04-20T10:05:00Z",
    "precisionTier": "STANDARD",
    "shardKey": "order-123",
    "callback": {
      "url": "https://httpbin.org/post",
      "method": "POST"
    }
  }'
```

## Single Node Deployment

### Recommended Layout

```text
/opt/loomq/
├── bin/
│   └── loomq-server.jar
├── config/
│   └── application.yml
├── data/
│   └── wal/
└── logs/
```

### Systemd Example

```ini
[Unit]
Description=LoomQ Standalone Server
After=network.target

[Service]
Type=simple
User=loomq
Group=loomq
WorkingDirectory=/opt/loomq
Environment="LOOMQ_SERVER_HOST=0.0.0.0"
Environment="LOOMQ_SERVER_PORT=8080"
Environment="LOOMQ_DATA_DIR=/opt/loomq/data/wal"
Environment="LOOMQ_NODE_ID=node-1"
ExecStart=/opt/loomq/bin/loomq-server.jar
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### JVM Settings

```bash
java -Xms2g -Xmx2g -XX:+UseZGC -jar loomq-server/target/loomq-server-0.7.0-SNAPSHOT.jar
```

For production, increase heap according to your pending-intent target and WAL retention.

## Container Deployment

### Docker

```bash
docker build -t loomq:0.7.0-SNAPSHOT .
docker run -d \
  --name loomq \
  -p 8080:8080 \
  -v /host/wal:/data/wal \
  -e LOOMQ_SERVER_PORT=8080 \
  -e LOOMQ_DATA_DIR=/data/wal \
  -e LOOMQ_NODE_ID=node-1 \
  loomq:0.7.0-SNAPSHOT
```

### Docker Compose

Use the repository `docker-compose.yml` if you want the bundled stack.

## Kubernetes Deployment

The current server is still a single-node standalone runtime, so Kubernetes should be treated as a packaging and orchestration layer rather than a full cluster-consensus setup.

### Example ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: loomq-config
data:
  application.yml: |
    server:
      host: "0.0.0.0"
      port: 8080
    wal:
      data_dir: "/data/wal"
      flush_strategy: "batch"
    scheduler:
      max_pending_tasks: 1000000
```

### Example Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loomq
spec:
  replicas: 1
  selector:
    matchLabels:
      app: loomq
  template:
    metadata:
      labels:
        app: loomq
    spec:
      containers:
        - name: loomq
          image: loomq:0.7.0-SNAPSHOT
          ports:
            - containerPort: 8080
          env:
            - name: LOOMQ_SERVER_HOST
              value: "0.0.0.0"
            - name: LOOMQ_SERVER_PORT
              value: "8080"
            - name: LOOMQ_DATA_DIR
              value: "/data/wal"
            - name: LOOMQ_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          volumeMounts:
            - name: wal
              mountPath: /data/wal
      volumes:
        - name: wal
          persistentVolumeClaim:
            claimName: loomq-wal-pvc
```

### Probe Paths

- Liveness: `GET /health/live`
- Readiness: `GET /health/ready`

## Monitoring Setup

### Metrics Endpoints

- `GET /metrics`
- `GET /api/v1/metrics`

### Prometheus

Point Prometheus at the standalone server and scrape the metrics endpoint. Useful signals include:

- `loomq_http_requests_total`
- `loomq_http_request_duration_seconds`
- `loomq_http_active_requests`
- `loomq_http_concurrency_limit_exceeded_total`
- `loomq_netty_active_connections`
- `loomq_netty_connection_errors_total`
- `loomq_tasks_created_total`
- `loomq_wal_record_count`
- `loomq_scheduler_bucket_size`
- `loomq_scheduler_wakeup_latency_ms_p99`
- `loomq_scheduler_wakeup_latency_ms_p999`

### Dashboard Ideas

- request rate
- active requests
- concurrency rejects
- WAL health
- intent creation rate
- WAL record count
- wakeup latency by precision tier

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOOMQ_SERVER_HOST` | `0.0.0.0` | HTTP bind address |
| `LOOMQ_SERVER_PORT` | `8080` | HTTP bind port |
| `LOOMQ_SERVER_BACKLOG` | `1024` | Socket backlog |
| `LOOMQ_VIRTUAL_THREADS` | `true` | Enable virtual threads |
| `LOOMQ_MAX_REQUEST_SIZE` | `10485760` | Maximum request size in bytes |
| `LOOMQ_THREAD_POOL_SIZE` | `200` | Fallback worker pool size |
| `LOOMQ_NETTY_PORT` | `8080` | Netty bind port |
| `LOOMQ_NETTY_HOST` | `0.0.0.0` | Netty bind address |
| `LOOMQ_WAL_DATA_DIR` | `./data/wal` | WAL data directory |
| `LOOMQ_WAL_FLUSH_STRATEGY` | `batch` | WAL flush strategy |
| `LOOMQ_SCHEDULER_MAX_PENDING` | `1000000` | Maximum pending intents |
| `LOOMQ_DISPATCHER_MAX_CONCURRENT` | `1000` | Concurrent dispatch limit |
| `LOOMQ_SHARD_INDEX` | `0` | Cluster shard index placeholder |
| `LOOMQ_TOTAL_SHARDS` | `1` | Total shards placeholder |
| `LOOMQ_NODE_ID` | `node-1` | Node identifier |
| `LOOMQ_DATA_DIR` | `./data/wal` | Overrides the WAL root directory used by the standalone server |

## Troubleshooting

### Server Won't Start

- verify port `8080` is free
- verify the WAL directory is writable
- confirm the Java version is 25+
- check the startup log for the runtime configuration summary

### Health Check Fails

- confirm `/health` and `/health/ready` return as expected
- inspect WAL activity and disk permissions
- check `walHealthy` in the metrics snapshot

### Requests Are Rejected

- inspect `loomq_http_concurrency_limit_exceeded_total`
- lower load or increase `netty.maxConcurrentBusinessRequests`
- check downstream callback latency

### Recovery Feels Slow

- reduce WAL retention if the data directory has grown too large
- increase recovery batch size only after measuring
- verify the snapshot directory under the WAL root

## Support

For issues and feature requests, use the repository issue tracker.
