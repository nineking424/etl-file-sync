---
name: kafka-expert
description: Design and optimize Apache Kafka configurations including topic/partition design, producer/consumer tuning, and Kubernetes deployment
---

# Kafka Expert Skill

## Overview

The Kafka Expert skill provides comprehensive guidance for designing, configuring, and optimizing Apache Kafka deployments. This skill covers topic and partition design strategies, producer and consumer tuning, Kubernetes deployment patterns, and troubleshooting common issues in production environments.

## Core Capabilities

### 1. Topic and Partition Design
- Determine optimal partition count based on throughput requirements
- Design partition key strategies for data distribution and ordering
- Calculate retention policies and storage requirements
- Design multi-topic architectures for complex event flows
- Implement compacted topics for changelog use cases

### 2. Consumer Group Optimization
- Design consumer group topology for scalability
- Implement rebalancing strategies and partition assignment
- Optimize offset management and commit strategies
- Handle consumer lag and backpressure scenarios
- Implement parallel processing patterns

### 3. Kubernetes LoadBalancer External Access
- Configure external access using LoadBalancer services
- Design network topology for external client connectivity
- Implement advertised listeners for multi-network environments
- Set up DNS and routing for broker discovery
- Configure security policies for external access

### 4. KRaft Mode Configuration
- Migrate from ZooKeeper to KRaft mode
- Design controller quorum topology
- Configure metadata logs and retention
- Implement disaster recovery for KRaft clusters
- Optimize controller performance and availability

### 5. Producer and Consumer Tuning
- Configure batching, compression, and throughput settings
- Optimize idempotence and exactly-once semantics
- Tune fetch sizes and polling intervals
- Configure retries, timeouts, and error handling
- Implement backpressure and flow control

## Workflow

When working with Kafka configurations:

### 1. Requirements Analysis
- Identify throughput, latency, and durability requirements
- Determine data ordering and partitioning needs
- Assess consumer group topology and scaling needs
- Evaluate network topology and access patterns
- Define SLA requirements for availability and recovery

### 2. Design Phase
- Design topic structure and partition strategy
- Plan consumer group architecture
- Design network and security topology
- Select deployment mode (ZooKeeper or KRaft)
- Design monitoring and alerting strategy

### 3. Implementation
- Create topic configurations with appropriate settings
- Deploy Kafka on Kubernetes with proper resource allocation
- Configure producer and consumer applications
- Set up monitoring and observability
- Implement backup and disaster recovery

### 4. Optimization
- Analyze performance metrics and bottlenecks
- Tune partition counts and replication factors
- Optimize producer and consumer configurations
- Adjust resource allocations (CPU, memory, storage)
- Implement capacity planning based on growth projections

### 5. Troubleshooting
- Diagnose consumer lag and rebalancing issues
- Identify and resolve broker performance problems
- Debug network connectivity and DNS issues
- Resolve data loss or duplication scenarios
- Handle cluster recovery and failover situations

## Checklist

Before finalizing Kafka configurations:

- [ ] Partition count aligns with expected throughput and consumer parallelism
- [ ] Retention policies match data lifecycle requirements
- [ ] Replication factor provides adequate durability (minimum 3 for production)
- [ ] Producer acks setting matches durability requirements (acks=all for critical data)
- [ ] Consumer offset commit strategy prevents data loss and duplication
- [ ] Kubernetes resource requests and limits are properly configured
- [ ] External access is properly secured with authentication and encryption
- [ ] Monitoring covers key metrics (lag, throughput, errors, broker health)
- [ ] Disaster recovery and backup procedures are documented and tested
- [ ] Performance testing validates throughput and latency requirements

## Usage Examples

### Example 1: Design High-Throughput Topic
```yaml
# Topic configuration for 1M msgs/sec throughput
topic: user-events
partitions: 100
replication-factor: 3
configs:
  compression.type: lz4
  min.insync.replicas: 2
  retention.ms: 604800000  # 7 days
  segment.ms: 3600000      # 1 hour
  max.message.bytes: 1048576

# Producer config
acks: all
compression.type: lz4
batch.size: 65536
linger.ms: 10
buffer.memory: 67108864
```

### Example 2: Configure External Access on Kubernetes
```yaml
# Kafka StatefulSet with LoadBalancer access
apiVersion: v1
kind: Service
metadata:
  name: kafka-0-external
spec:
  type: LoadBalancer
  selector:
    app: kafka
    statefulset.kubernetes.io/pod-name: kafka-0
  ports:
  - port: 9094
    targetPort: 9094
    protocol: TCP

# Kafka broker config
advertised.listeners: INTERNAL://kafka-0.kafka-headless:9092,EXTERNAL://external-lb-dns:9094
listener.security.protocol.map: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name: INTERNAL
```

### Example 3: Optimize Consumer Group
```properties
# Consumer configuration for optimal throughput
group.id: high-throughput-consumers
fetch.min.bytes: 524288
fetch.max.wait.ms: 500
max.partition.fetch.bytes: 1048576
enable.auto.commit: false
max.poll.records: 500
session.timeout.ms: 30000
heartbeat.interval.ms: 3000
```

### Example 4: KRaft Mode Configuration
```properties
# KRaft controller configuration
process.roles: controller,broker
node.id: 1
controller.quorum.voters: 1@kafka-0:9093,2@kafka-1:9093,3@kafka-2:9093
controller.listener.names: CONTROLLER
listeners: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093

# Metadata configuration
metadata.log.dir: /var/lib/kafka/metadata
metadata.log.segment.ms: 3600000
metadata.max.retention.ms: 604800000
```

## Integration Points

### With Airflow Architect
- Design Kafka consumer DAGs for stream processing
- Implement exactly-once processing in Airflow tasks
- Configure task parallelism based on partition count
- Monitor Kafka lag from Airflow sensors

### With ETL Pipeline Builder
- Design CDC patterns using Kafka Connect
- Implement idempotent data ingestion from Kafka topics
- Handle schema evolution with Schema Registry
- Design error handling and dead letter queues

### With K8s Specialist
- Optimize Kafka StatefulSet configurations
- Design PVC storage classes for broker data
- Configure HPA based on consumer lag metrics
- Implement network policies for Kafka security

### With Database Optimizer
- Design CDC pipelines from Oracle to Kafka
- Optimize Kafka Connect JDBC source/sink connectors
- Tune batch sizes for database writes from Kafka
- Implement exactly-once delivery to databases

## References

See the `references/` directory for detailed guides on:
- **topic-design-patterns.md**: Topic naming, partition strategies, and retention policies
- **partition-strategies.md**: Partition key selection and data distribution patterns
- **consumer-patterns.md**: Consumer group design and offset management strategies
- **k8s-kafka-setup.md**: Kubernetes deployment with StatefulSets and external access
- **troubleshooting.md**: Common issues, diagnostics, and resolution strategies

## Best Practices

1. **Always use replication factor >= 3** for production topics
2. **Enable idempotence** for producers to prevent duplicates
3. **Use consumer groups** for parallel processing and fault tolerance
4. **Monitor consumer lag** as a primary health metric
5. **Implement proper shutdown handling** to prevent data loss
6. **Use compression** to reduce network and storage costs
7. **Test failover scenarios** regularly to validate HA setup
8. **Document partition key strategy** for maintenance and troubleshooting
9. **Use Schema Registry** for structured data to manage evolution
10. **Implement circuit breakers** for downstream system failures
