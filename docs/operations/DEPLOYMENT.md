# LoomQ Deployment Guide

Version: `0.9.0`

This guide matches the current standalone server, scheduler, persistence, and Raft deployment layout.

## Quick Start

### Prerequisites

- JDK 25+
- Maven 3.9+
- Docker 24+ if you plan to use containers

### Build

```bash
git clone https://github.com/loomq/loomq.git
cd loomq
mvn clean package -DskipTests
```

### Run

```bash
java -jar loomq-server/target/loomq-server-0.9.0.jar
```

The server listens on `http://localhost:7928` by default.

### Verify

```bash
curl http://localhost:7928/health
curl http://localhost:7928/health/ready
curl http://localhost:7928/metrics
```

Create an `Intent`:

```bash
curl -X POST http://localhost:7928/v1/intents \
  -H "Content-Type: application/json" \
  -d '{
    "executeAt": "2026-05-13T12:00:00Z",
    "precisionTier": "STANDARD",
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
Environment="LOOMQ_SERVER_PORT=7928"
Environment="LOOMQ_DATA_DIR=/opt/loomq/data/wal"
Environment="LOOMQ_NODE_ID=node-1"
Environment="LOOMQ_SCHEDULER_MAX_PENDING_INTENTS=1000000"
ExecStart=/usr/bin/java -jar /opt/loomq/bin/loomq-server.jar
Restart=on-failure
RestartSec=1
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### JVM Settings

```bash
java -Xms2g -Xmx2g -XX:+UseZGC -jar loomq-server/target/loomq-server-0.9.0.jar
```

Tune heap size according to your pending-intent target and WAL retention window.

## Raft Deployment

Raft is enabled through the server process and should be deployed with stable node identities.

### Required Variables

| Variable | Purpose |
|----------|---------|
| `LOOMQ_RAFT_ENABLED` | Enable Raft mode |
| `LOOMQ_RAFT_NODE_ID` | Local Raft node ID |
| `LOOMQ_RAFT_PEERS` | Peer list, supports `peerId@host:port` |
| `LOOMQ_RAFT_PORT` | Local Raft RPC bind port |
| `LOOMQ_DATA_DIR` | WAL and snapshot location |

### Example

```bash
export LOOMQ_RAFT_ENABLED=true
export LOOMQ_RAFT_NODE_ID=node-1
export LOOMQ_RAFT_PEERS="node-1@10.0.0.11:7930,node-2@10.0.0.12:7930,node-3@10.0.0.13:7930"
export LOOMQ_RAFT_PORT=7930
./scripts/start.sh
```

### Notes

- Use a stable host name or IP for every node.
- Open the Raft port between peers before starting the Raft group.
- Keep `LOOMQ_DATA_DIR` persistent so snapshots and WAL metadata survive restarts.

## Container Deployment

### Docker

```bash
docker build -t loomq:0.9.0 .
docker run -d \
  --name loomq \
  -p 7928:7928 \
  -v /host/wal:/data/wal \
  -e LOOMQ_SERVER_PORT=7928 \
  -e LOOMQ_DATA_DIR=/data/wal \
  -e LOOMQ_NODE_ID=node-1 \
  -e LOOMQ_SCHEDULER_MAX_PENDING_INTENTS=1000000 \
  loomq:0.9.0
```

### Docker Compose

Use the repository `docker-compose.yml` for the single-node stack and optional monitoring profile.

## Kubernetes Deployment

Treat Kubernetes as the orchestration layer for a stable StatefulSet deployment.

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
      port: 7928
    wal:
      data_dir: "/data/wal"
    scheduler:
      max_pending_intents: 1000000
```

### Example StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: loomq
spec:
  serviceName: loomq-headless
  replicas: 3
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
          image: loomq:0.9.0
          ports:
            - containerPort: 7928
            - containerPort: 7930
          env:
            - name: LOOMQ_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: LOOMQ_RAFT_ENABLED
              value: "true"
            - name: LOOMQ_RAFT_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: LOOMQ_RAFT_PORT
              value: "7930"
            - name: LOOMQ_RAFT_PEERS
              value: "loomq-0@loomq-0.loomq-headless:7930,loomq-1@loomq-1.loomq-headless:7930,loomq-2@loomq-2.loomq-headless:7930"
            - name: LOOMQ_DATA_DIR
              value: /data/wal
            - name: LOOMQ_SCHEDULER_MAX_PENDING_INTENTS
              value: "1000000"
          volumeMounts:
            - name: wal
              mountPath: /data/wal
  volumeClaimTemplates:
    - metadata:
        name: wal
      spec:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 10Gi
```

### Probe Paths

- Liveness: `GET /health/live`
- Readiness: `GET /health/ready`

## Monitoring Setup

### Metrics Endpoints

- `GET /metrics`
- `GET /api/v1/metrics`

### Prometheus

Point Prometheus at the server and scrape the metrics endpoint. Useful signals include:

- `loomq_http_requests_total`
- `loomq_http_request_duration_seconds`
- `loomq_http_active_requests`
- `loomq_http_concurrency_limit_exceeded_total`
- `loomq_intents_created_total`
- `loomq_intents_ack_success_total`
- `loomq_intents_pending`
- `loomq_scheduler_max_pending_intents`
- `loomq_wal_record_count`
- `loomq_scheduler_bucket_size`
- `loomq_scheduler_wakeup_latency_ms_p95`
- `loomq_scheduler_wakeup_latency_ms_p99`
- `loomq_scheduler_wakeup_latency_ms_p999`

## Release Checklist

Before publishing a GitHub release:

1. Run the full Maven test suite.
2. Verify the packaged jar name matches `loomq-server-0.9.0.jar`.
3. Confirm the README, changelog, deployment guide, and scripts all point at the same version.
4. Validate at least one single-node and one Raft-style startup path.
