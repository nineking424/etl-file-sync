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

# Worker configuration (default: 1 worker)
ETL_WORKER_COUNT="${ETL_WORKER_COUNT:-1}"
KAFKA_TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-4}"

echo "============================================"
echo "ETL File Sync Container Starting..."
echo "============================================"
echo "Topic: ${ETL_TOPIC}"
echo "Group ID: ${ETL_GROUP_ID}"
echo "Bootstrap Servers: ${ETL_BOOTSTRAP_SERVERS}"
echo "Worker Count: ${ETL_WORKER_COUNT}"
echo "Topic Partitions: ${KAFKA_TOPIC_PARTITIONS}"
echo "============================================"

# Export environment variables for supervisord
export ETL_TOPIC
export ETL_GROUP_ID
export ETL_BOOTSTRAP_SERVERS
export ETL_WORKER_COUNT
export KAFKA_TOPIC_PARTITIONS

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

# Create topics with configured partitions (run in background after Kafka starts)
create_topics() {
    # Wait for Kafka to be ready
    echo "Waiting for Kafka to be ready..."
    sleep 20

    # Create main topic
    ${KAFKA_HOME}/bin/kafka-topics.sh --create \
        --topic ${ETL_TOPIC} \
        --partitions ${KAFKA_TOPIC_PARTITIONS} \
        --replication-factor 1 \
        --if-not-exists \
        --bootstrap-server ${ETL_BOOTSTRAP_SERVERS} 2>/dev/null || true

    # Create DLQ topic
    ${KAFKA_HOME}/bin/kafka-topics.sh --create \
        --topic ${ETL_TOPIC}-dlq \
        --partitions 1 \
        --replication-factor 1 \
        --if-not-exists \
        --bootstrap-server ${ETL_BOOTSTRAP_SERVERS} 2>/dev/null || true

    echo "Topics created successfully."
}

# Start topic creation in background
create_topics &

echo "============================================"
echo "Starting supervisord..."
echo "============================================"

# Start supervisord (manages Kafka and ETL processes)
exec /usr/bin/supervisord -c /etc/supervisord.conf
