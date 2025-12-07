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

echo "============================================"
echo "ETL File Sync Container Starting..."
echo "============================================"
echo "Topic: ${ETL_TOPIC}"
echo "Group ID: ${ETL_GROUP_ID}"
echo "Bootstrap Servers: ${ETL_BOOTSTRAP_SERVERS}"
echo "============================================"

# Export environment variables for supervisord
export ETL_TOPIC
export ETL_GROUP_ID
export ETL_BOOTSTRAP_SERVERS

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

echo "============================================"
echo "Starting supervisord..."
echo "============================================"

# Start supervisord (manages Kafka and ETL processes)
exec /usr/bin/supervisord -c /etc/supervisord.conf
