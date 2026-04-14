# LoomQ - High-Performance Delayed Task Queue
# Java 21 Virtual Threads based distributed task scheduler

FROM eclipse-temurin:21-jdk-alpine

LABEL maintainer="loomq"
LABEL version="0.6.0"
LABEL description="LoomQ Delayed Task Queue Engine"

# Install necessary tools
RUN apk add --no-cache curl jq

# Create app directory
WORKDIR /app

# Create data directory for WAL
RUN mkdir -p /app/data/wal

# Copy the shaded JAR
COPY target/loomq-0.6.0.jar app.jar

# Copy default configuration
COPY src/main/resources/application.yml config/application.yml

# Environment variables with defaults
ENV LOOMQ_SERVER_HOST=0.0.0.0
ENV LOOMQ_SERVER_PORT=8080
ENV LOOMQ_WAL_DATA_DIR=/app/data/wal
ENV LOOMQ_WAL_FLUSH_STRATEGY=batch
ENV LOOMQ_SCHEDULER_MAX_PENDING=1000000
ENV LOOMQ_DISPATCHER_MAX_CONCURRENT=1000
ENV JVM_XMS=2g
ENV JVM_XMX=2g
ENV JVM_GC=ZGC
ENV JVM_GC_PAUSE=10

# Expose ports
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# JVM arguments for virtual threads and ZGC
ENTRYPOINT ["sh", "-c", \
    "java \
    -Xms${JVM_XMS} \
    -Xmx${JVM_XMX} \
    -XX:+Use${JVM_GC} \
    -XX:MaxGCPauseMillis=${JVM_GC_PAUSE} \
    --enable-preview \
    -Dloomq.server.host=${LOOMQ_SERVER_HOST} \
    -Dloomq.server.port=${LOOMQ_SERVER_PORT} \
    -Dloomq.wal.data_dir=${LOOMQ_WAL_DATA_DIR} \
    -Dloomq.wal.flush_strategy=${LOOMQ_WAL_FLUSH_STRATEGY} \
    -Dloomq.scheduler.max_pending_tasks=${LOOMQ_SCHEDULER_MAX_PENDING} \
    -Dloomq.dispatcher.max_concurrent_dispatches=${LOOMQ_DISPATCHER_MAX_CONCURRENT} \
    -jar app.jar \
    --config=config/application.yml \
    ${LOOMQ_EXTRA_ARGS}"]
