# ETL File Sync Container
# Rocky Linux 8 + Python 3.12 + Kafka 4.1.1 (KRaft mode)

FROM rockylinux:8

LABEL maintainer="ETL File Sync"
LABEL description="Single container with Kafka broker and Python ETL consumer"

# Environment variables
ENV KAFKA_VERSION=4.1.1
ENV SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk
ENV PATH="${KAFKA_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

# Install system dependencies
RUN dnf -y update && \
    dnf -y install \
        java-21-openjdk \
        java-21-openjdk-devel \
        python3.12 \
        python3.12-pip \
        python3.12-devel \
        supervisor \
        wget \
        tar \
        gzip \
        which \
        procps-ng \
        net-tools \
    && dnf clean all

# Set Python 3.12 as default
RUN alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1 && \
    alternatives --install /usr/bin/pip3 pip3 /usr/bin/pip3.12 1

# Download and install Kafka
RUN wget -q "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
        -O /tmp/kafka.tgz && \
    mkdir -p ${KAFKA_HOME} && \
    tar -xzf /tmp/kafka.tgz -C ${KAFKA_HOME} --strip-components=1 && \
    rm /tmp/kafka.tgz

# Create directories
RUN mkdir -p /var/kafka-logs \
             /var/log/supervisor \
             /app

# Copy Kafka configuration
COPY kafka/config/server.properties ${KAFKA_HOME}/config/kraft/server.properties

# Copy application code
COPY src/ /app/src/
COPY requirements.txt /app/

# Install Python dependencies
WORKDIR /app
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy supervisor configuration
COPY docker/supervisord.conf /etc/supervisord.conf

# Copy entrypoint script
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose Kafka ports
EXPOSE 9092 9093

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]
