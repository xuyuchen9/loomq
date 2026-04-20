# LoomQ Deployment Guide
# Version: 0.7.0-SNAPSHOT

## Table of Contents

1. [Quick Start](#quick-start)
2. [Single Node Deployment](#single-node-deployment)
3. [Cluster Deployment](#cluster-deployment)
4. [Docker Deployment](#docker-deployment)
5. [Kubernetes Deployment](#kubernetes-deployment)
6. [Monitoring Setup](#monitoring-setup)
7. [Configuration Reference](#configuration-reference)
8. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Prerequisites

- Java 21+ (OpenJDK or Eclipse Temurin)
- Maven 3.8+ (for building from source)
- Docker 20.10+ (for containerized deployment)
- 4GB+ RAM available

### 1. Build from Source

```bash
# Clone repository
git clone https://github.com/yourusername/loomq.git
cd loomq

# Build
mvn clean package -DskipTests

# Run
java --enable-preview -jar target/loomq-0.1.0-SNAPSHOT-shaded.jar
```

### 2. Docker Quick Start

```bash
# Pull and run
docker run -d -p 8080:8080 loomq:0.7.0-SNAPSHOT

# Or use docker-compose
docker-compose up -d
```

### 3. Verify Installation

```bash
# Health check
curl http://localhost:8080/health

# Create a test task
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"callbackUrl":"https://httpbin.org/post","delayMillis":5000}'
```

---

## Single Node Deployment

### System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 2 cores | 4+ cores |
| RAM | 4GB | 8GB |
| Disk | 10GB | 50GB+ SSD |
| Network | 100Mbps | 1Gbps |

### JVM Configuration

```bash
# Development
java -Xms2g -Xmx2g \
     -XX:+UseZGC \
     --enable-preview \
     -jar loomq.jar

# Production (8GB heap)
java -Xms8g -Xmx8g \
     -XX:+UseZGC \
     -XX:MaxGCPauseMillis=10 \
     -XX:+DisableExplicitGC \
     --enable-preview \
     -jar loomq.jar

# Production (16GB heap with large pages)
java -Xms16g -Xmx16g \
     -XX:+UseZGC \
     -XX:+UseLargePages \
     -XX:MaxGCPauseMillis=10 \
     --enable-preview \
     -jar loomq.jar
```

### Directory Structure

```
/opt/loomq/
├── bin/
│   ├── loomq.jar
│   └── start.sh
├── config/
│   └── application.yml
├── data/
│   └── wal/           # WAL segments
├── logs/
│   ├── loomq.log
│   ├── audit.log
│   ├── wal.log
│   └── scheduler.log
└── scripts/
    └── backup.sh
```

### Systemd Service

Create `/etc/systemd/system/loomq.service`:

```ini
[Unit]
Description=LoomQ Delayed Task Queue
After=network.target

[Service]
Type=simple
User=loomq
Group=loomq
WorkingDirectory=/opt/loomq
Environment="JVM_XMS=8g"
Environment="JVM_XMX=8g"
Environment="JVM_GC=ZGC"
Environment="LOOMQ_PORT=8080"
Environment="LOOMQ_DATA_DIR=/opt/loomq/data/wal"
ExecStart=/opt/loomq/bin/start.sh
ExecStop=/bin/kill -TERM $MAINPID
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=loomq

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable loomq
sudo systemctl start loomq
sudo systemctl status loomq
```

---

## Cluster Deployment

### 3-Node Cluster Example

**Node 1 (192.168.1.10):**

```bash
export LOOMQ_SHARD_INDEX=0
export LOOMQ_TOTAL_SHARDS=3
export LOOMQ_NODES="shard-0:192.168.1.10:8080,shard-1:192.168.1.11:8080,shard-2:192.168.1.12:8080"
export LOOMQ_PORT=8080
./scripts/start.sh
```

**Node 2 (192.168.1.11):**

```bash
export LOOMQ_SHARD_INDEX=1
export LOOMQ_TOTAL_SHARDS=3
export LOOMQ_NODES="shard-0:192.168.1.10:8080,shard-1:192.168.1.11:8080,shard-2:192.168.1.12:8080"
export LOOMQ_PORT=8080
./scripts/start.sh
```

**Node 3 (192.168.1.12):**

```bash
export LOOMQ_SHARD_INDEX=2
export LOOMQ_TOTAL_SHARDS=3
export LOOMQ_NODES="shard-0:192.168.1.10:8080,shard-1:192.168.1.11:8080,shard-2:192.168.1.12:8080"
export LOOMQ_PORT=8080
./scripts/start.sh
```

### Load Balancer Configuration

**Nginx Example:**

```nginx
upstream loomq_cluster {
    least_conn;
    server 192.168.1.10:8080;
    server 192.168.1.11:8080;
    server 192.168.1.12:8080;

    keepalive 32;
}

server {
    listen 80;

    location / {
        proxy_pass http://loomq_cluster;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_connect_timeout 5s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    location /health {
        access_log off;
        proxy_pass http://loomq_cluster/health;
    }
}
```

---

## Docker Deployment

### Single Container

```bash
# Build
docker build -t loomq:0.7.0-SNAPSHOT .

# Run with custom config
docker run -d \
  --name loomq \
  -p 8080:8080 \
  -v /host/data:/app/data/wal \
  -v /host/config:/app/config \
  -e JVM_XMS=4g \
  -e JVM_XMX=4g \
  -e LOOMQ_WAL_FLUSH_STRATEGY=batch \
  loomq:0.7.0-SNAPSHOT
```

### Docker Compose

**Single Node:**

```bash
docker-compose up -d
```

**With Monitoring:**

```bash
docker-compose --profile monitoring up -d
```

**Cluster Mode:**

```bash
docker-compose --profile cluster up -d
```

### Docker Swarm

```bash
# Initialize swarm
docker swarm init

# Deploy stack
docker stack deploy -c docker-compose.yml loomq

# Scale
docker service scale loomq_loomq-single=3
```

---

## Kubernetes Deployment

### Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: loomq
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: loomq-config
  namespace: loomq
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
    cluster:
      enabled: true
```

### StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: loomq
  namespace: loomq
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
        image: loomq:0.7.0-SNAPSHOT
        ports:
        - containerPort: 8080
          name: http
        env:
        - name: LOOMQ_SHARD_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: LOOMQ_TOTAL_SHARDS
          value: "3"
        - name: JVM_XMS
          value: "2g"
        - name: JVM_XMX
          value: "2g"
        volumeMounts:
        - name: data
          mountPath: /data/wal
        - name: config
          mountPath: /app/config
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config
        configMap:
          name: loomq-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: loomq
  namespace: loomq
spec:
  selector:
    app: loomq
  ports:
  - port: 8080
    targetPort: 8080
  type: LoadBalancer
```

Deploy:

```bash
kubectl apply -f k8s/
```

---

## Monitoring Setup

### Prometheus

1. Edit `monitoring/prometheus.yml` with your targets
2. Start Prometheus:

```bash
docker run -d \
  --name prometheus \
  -p 9090:9090 \
  -v $(pwd)/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus:v2.50.0
```

### Grafana

1. Import dashboard from `monitoring/grafana/dashboards/`
2. Configure Prometheus datasource
3. Access at http://localhost:3000

### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `loomq_tasks_created_total` | Task creation rate | - |
| `loomq_tasks_dispatched_total` | Task dispatch rate | - |
| `loomq_scheduler_pending_count` | Pending tasks | > 800000 |
| `loomq_wal_write_latency` | WAL write latency | P99 > 100ms |
| `loomq_dispatcher_queue_size` | Dispatch queue size | > 900 |
| `jvm_memory_used_bytes` | JVM memory usage | > 80% |

---

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOOMQ_SERVER_HOST` | `0.0.0.0` | Server bind address |
| `LOOMQ_SERVER_PORT` | `8080` | Server port |
| `LOOMQ_DATA_DIR` | `./data/wal` | WAL data directory |
| `LOOMQ_WAL_FLUSH_STRATEGY` | `batch` | Flush strategy |
| `LOOMQ_SCHEDULER_MAX_PENDING` | `1000000` | Max pending tasks |
| `LOOMQ_DISPATCHER_MAX_CONCURRENT` | `1000` | Max concurrent dispatches |
| `LOOMQ_SHARD_INDEX` | `0` | Shard index (cluster mode) |
| `LOOMQ_TOTAL_SHARDS` | `1` | Total shards |
| `LOOMQ_NODES` | - | Cluster nodes list |
| `JVM_XMS` | `2g` | JVM initial heap |
| `JVM_XMX` | `2g` | JVM max heap |
| `JVM_GC` | `ZGC` | Garbage collector |

---

## Troubleshooting

### High Memory Usage

```bash
# Check heap usage
curl http://localhost:8080/metrics | grep jvm_memory_used_bytes

# Adjust heap size
export JVM_XMX=16g
```

### WAL Recovery Issues

```bash
# Check WAL segments
ls -la data/wal/

# Enable safe mode
export LOOMQ_RECOVERY_SAFE_MODE=true
```

### Network Issues

```bash
# Test connectivity
curl -v http://localhost:8080/health

# Check firewall
sudo iptables -L -n | grep 8080
```

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Address already in use` | Port 8080 occupied | Change `LOOMQ_PORT` |
| `OutOfMemoryError` | Heap too small | Increase `JVM_XMX` |
| `Permission denied` | Wrong file permissions | `chmod 755 /opt/loomq` |
| `Java version mismatch` | Java < 21 | Install Java 21+ |

---

## Support

For issues and feature requests, please visit:
https://github.com/yourusername/loomq/issues
