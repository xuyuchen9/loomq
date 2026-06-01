# LoomQ Deployment Guide

Version: `0.9.2`

This guide matches the current standalone server, scheduler, persistence, and Raft deployment layout.

## Quick Start

### Prerequisites

- JDK 25+
- Maven 3.9+
- Docker 24+ if you plan to use containers

### Build

```bash
git clone https://github.com/xuyuchen9/loomq.git
cd loomq
mvn clean package -DskipTests
```

### Run

```bash
java -jar loomq-server/target/loomq-server-0.9.2.jar
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

If `LOOMQ_SECURITY_ENABLED=true`, add `-H "X-Loomq-Token: ${LOOMQ_API_TOKEN}"` to `/v1/**` and metrics requests.

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
Environment="LOOMQ_SECURITY_ENABLED=true"
Environment="LOOMQ_API_TOKEN=replace-with-secret"
Environment="LOOMQ_SCHEDULER_MAX_PENDING_INTENTS=1000000"
ExecStart=/usr/bin/java -jar /opt/loomq/bin/loomq-server.jar
Restart=on-failure
RestartSec=1
TimeoutStopSec=35
KillSignal=SIGTERM
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### JVM Settings

```bash
java -Xms2g -Xmx2g -XX:+UseZGC -jar loomq-server/target/loomq-server-0.9.2.jar
```

Tune heap size according to your pending-intent target and WAL retention window.

### Graceful Shutdown

LoomQ handles `SIGTERM` by closing the HTTP listener first, waiting for active HTTP business requests up to
`netty.gracefulShutdownTimeoutMs`, then closing callback, Raft, WAL, and engine resources. Set the orchestrator stop
window slightly above `netty.gracefulShutdownTimeoutMs`; for the default 30 seconds, use at least 35 seconds.

## Raft Deployment

Raft is enabled through the server process and should be deployed with stable node identities.
In Raft mode, `GET /v1/intents/{intentId}` is leader-authoritative; followers return a retryable 503 with a leader hint when known.
The leader only serves reads while its quorum freshness lease is valid, so a node that has lost contact with a majority will stop answering reads before it risks serving stale state.

### Required Variables

| Variable | Purpose |
|----------|---------|
| `LOOMQ_RAFT_ENABLED` | Enable Raft mode |
| `LOOMQ_RAFT_NODE_ID` | Local Raft node ID |
| `LOOMQ_RAFT_PEERS` | Peer list, self-only lists can contain just the local node id; remote peers should use `peerId@host:port` or `peerId=host:port` |
| `LOOMQ_RAFT_PORT` | Local Raft RPC bind port |
| `LOOMQ_DATA_DIR` | WAL and snapshot location |

### Example

```bash
export LOOMQ_RAFT_ENABLED=true
export LOOMQ_RAFT_NODE_ID=node-1
export LOOMQ_RAFT_PEERS="node-1@10.0.0.11:9928,node-2@10.0.0.12:9928,node-3@10.0.0.13:9928"
export LOOMQ_RAFT_PORT=9928
./scripts/start.sh
```

### Notes

- Use a stable host name or IP for every node.
- Open the Raft port between peers before starting the Raft group.
- Keep `LOOMQ_DATA_DIR` persistent so snapshots and WAL metadata survive restarts.
- Verify `/health` and `/metrics` after startup; both endpoints expose Raft role, leader id, term, commit index, commit lag, replication lag, peer reachability, and whether the leader is currently accepting reads / writes.
- Use `/health/live` only for process liveness. Use `/health/ready` for client traffic readiness; in Raft mode followers and leaders without quorum/read lease/zero commit lag return HTTP 503 with a machine-readable reason.

## Container Deployment

### Docker

```bash
docker build -t loomq:0.9.2 .
docker run -d \
  --name loomq \
  -p 7928:7928 \
  -v /host/wal:/data/wal \
  -e LOOMQ_SERVER_PORT=7928 \
  -e LOOMQ_DATA_DIR=/data/wal \
  -e LOOMQ_NODE_ID=node-1 \
  -e LOOMQ_SECURITY_ENABLED=true \
  -e LOOMQ_API_TOKEN=replace-with-secret \
  -e LOOMQ_SCHEDULER_MAX_PENDING_INTENTS=1000000 \
  loomq:0.9.2
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
          image: loomq:0.9.2
          ports:
            - containerPort: 7928
            - containerPort: 9928
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
              value: "9928"
            - name: LOOMQ_RAFT_PEERS
              value: "loomq-0@loomq-0.loomq-headless:9928,loomq-1@loomq-1.loomq-headless:9928,loomq-2@loomq-2.loomq-headless:9928"
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

In Raft mode, readiness is leader-only by design. This keeps generic load balancers from sending client reads or writes to followers that would reject them anyway.

## Monitoring Setup

### Health / Metrics Endpoints

- `GET /metrics`
- `GET /api/v1/metrics`
- `GET /health`
- `GET /health/deep`

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
- `raftRole`
- `raftLeaderId`
- `raftTerm`
- `raftCommitIndex`
- `raftLastApplied`
- `raftCommitLag`
- `raftReplicationLag`
- `raftConnectedPeers`
- `raftTotalPeers`
- `loomq_scheduler_wakeup_latency_ms_p99`
- `loomq_scheduler_wakeup_latency_ms_p999`

## Release Checklist

Before publishing a GitHub release:

1. Run the formatting gate (`make check-format` or `mvn -B -ntp com.diffplug.spotless:spotless-maven-plugin:3.0.0:check`).
2. Run the full Maven test suite.
3. Verify the packaged jar name matches `loomq-server-0.9.2.jar`.
4. Confirm the README, changelog, deployment guide, and scripts all point at the same version.
5. Validate at least one single-node and one Raft-style startup path.
