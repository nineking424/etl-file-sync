#!/bin/bash
set -e

KAFKA_HOME=/opt/kafka
KAFKA_CONFIG=${KAFKA_HOME}/config/kraft/server.properties
KAFKA_LOG_DIRS=/var/kafka-logs
CLUSTER_ID_FILE=${KAFKA_LOG_DIRS}/.cluster_id

# Parse command line arguments
ETL_TOPIC="${1:-file-transfer-jobs}"
ETL_GROUP_ID="${2:-etl-worker-group}"
ETL_BOOTSTRAP_SERVERS="${3:-localhost:9092}"

# Multi-consumer configuration (from environment variables)
ETL_CONSUMER_COUNT="${ETL_CONSUMER_COUNT:-1}"
ETL_NUM_PARTITIONS="${ETL_NUM_PARTITIONS:-4}"
DLQ_TOPIC_SUFFIX="${DLQ_TOPIC_SUFFIX:--dlq}"

echo "============================================"
echo "ETL File Sync Container Starting..."
echo "============================================"
echo "Topic: ${ETL_TOPIC}"
echo "Group ID: ${ETL_GROUP_ID}"
echo "Bootstrap Servers: ${ETL_BOOTSTRAP_SERVERS}"
echo "Consumer Count: ${ETL_CONSUMER_COUNT}"
echo "Partition Count: ${ETL_NUM_PARTITIONS}"
echo "============================================"

# Export environment variables for supervisord
export ETL_TOPIC
export ETL_GROUP_ID
export ETL_BOOTSTRAP_SERVERS

# Generate supervisord consumer configuration
generate_consumer_config() {
    local count=$1
    local config_file="/etc/supervisord.d/etl-consumers.conf"

    echo "Generating consumer configuration for ${count} instance(s)..."

    cat > ${config_file} << EOF
; Auto-generated ETL consumer configuration
; Consumer count: ${count}

[program:etl-consumer]
command=/usr/bin/python3.12 -m etl.main %(ENV_ETL_TOPIC)s %(ENV_ETL_GROUP_ID)s %(ENV_ETL_BOOTSTRAP_SERVERS)s
directory=/app/src
autostart=true
autorestart=true
startsecs=15
startretries=3
stopwaitsecs=10
stdout_logfile=/var/log/supervisor/etl-consumer-%(process_num)02d.log
stderr_logfile=/var/log/supervisor/etl-consumer-%(process_num)02d-error.log
stdout_logfile_maxbytes=50MB
stderr_logfile_maxbytes=50MB
priority=200
numprocs=${count}
process_name=%(program_name)s_%(process_num)02d
EOF

    echo "Consumer configuration generated: ${config_file}"
}

# Create topic if not exists (runs in background after Kafka starts)
create_topics_if_not_exists() {
    local topic=$1
    local partitions=$2
    local dlq_topic="${topic}${DLQ_TOPIC_SUFFIX}"

    echo "Waiting for Kafka to be ready..."

    # Wait for Kafka to be ready (max 60 seconds)
    local max_attempts=60
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if ${KAFKA_HOME}/bin/kafka-broker-api-versions.sh \
            --bootstrap-server localhost:9092 > /dev/null 2>&1; then
            echo "Kafka is ready!"
            break
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    if [ $attempt -eq $max_attempts ]; then
        echo "WARNING: Kafka did not become ready in time. Topic creation skipped."
        return 1
    fi

    # Create main topic
    echo "Creating topic: ${topic} with ${partitions} partition(s)..."
    ${KAFKA_HOME}/bin/kafka-topics.sh --create \
        --topic ${topic} \
        --partitions ${partitions} \
        --replication-factor 1 \
        --if-not-exists \
        --bootstrap-server localhost:9092 2>/dev/null || true

    # Create DLQ topic (1 partition)
    echo "Creating DLQ topic: ${dlq_topic}..."
    ${KAFKA_HOME}/bin/kafka-topics.sh --create \
        --topic ${dlq_topic} \
        --partitions 1 \
        --replication-factor 1 \
        --if-not-exists \
        --bootstrap-server localhost:9092 2>/dev/null || true

    # Describe created topics
    echo "============================================"
    echo "Topic Configuration:"
    ${KAFKA_HOME}/bin/kafka-topics.sh --describe \
        --topic ${topic} \
        --bootstrap-server localhost:9092 2>/dev/null || true
    echo "============================================"
}

# Generate consumer configuration
generate_consumer_config ${ETL_CONSUMER_COUNT}

# Initialize Kafka storage (KRaft mode) - only on first run
if [ ! -f "${CLUSTER_ID_FILE}" ]; then
    echo "Initializing Kafka KRaft storage (first run)..."

    # Generate new cluster ID
    CLUSTER_ID=$(${KAFKA_HOME}/bin/kafka-storage.sh random-uuid)
    echo "Generated Cluster ID: ${CLUSTER_ID}"

    # Format storage directory
    ${KAFKA_HOME}/bin/kafka-storage.sh format \
        --standalone \
        --cluster-id ${CLUSTER_ID} \
        --config ${KAFKA_CONFIG}

    # Save cluster ID for reference
    echo "${CLUSTER_ID}" > ${CLUSTER_ID_FILE}

    echo "Kafka storage initialized successfully."
else
    CLUSTER_ID=$(cat ${CLUSTER_ID_FILE})
    echo "Using existing Cluster ID: ${CLUSTER_ID}"
fi

# Start topic creation in background (after supervisord starts Kafka)
(
    sleep 5  # Wait for supervisord to start Kafka
    create_topics_if_not_exists "${ETL_TOPIC}" "${ETL_NUM_PARTITIONS}"
) &

echo "============================================"
echo "Starting supervisord..."
echo "============================================"

# Start supervisord (manages Kafka and ETL processes)
exec /usr/bin/supervisord -c /etc/supervisord.conf
