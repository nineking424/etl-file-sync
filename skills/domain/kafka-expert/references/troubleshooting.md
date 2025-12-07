# Kafka Troubleshooting Guide

Comprehensive guide to diagnosing and resolving common Apache Kafka issues in production environments.

## Consumer Lag Issues

### Symptom: High Consumer Lag
```bash
# Check lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --describe

# Output shows high LAG values
GROUP    TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
my-group my-topic   0          1000            10000           9000
```

### Root Causes and Solutions

#### 1. Slow Processing
**Diagnosis:**
```bash
# Check processing time per message
# Monitor consumer metrics
records-consumed-rate: Low
fetch-latency-avg: High
```

**Solutions:**
```properties
# Increase max.poll.records
max.poll.records=100  # Reduce batch size

# Increase poll timeout
max.poll.interval.ms=600000  # Allow more time

# Add parallelism
# Scale out consumer instances
```

#### 2. Insufficient Consumers
**Diagnosis:**
```bash
# Consumers < Partitions
Partitions: 30
Active consumers: 10
# Some consumers handle multiple partitions
```

**Solution:**
```bash
# Scale up to match partitions
kubectl scale deployment consumer --replicas=30
```

#### 3. Rebalancing Issues
**Diagnosis:**
```logs
Consumer group rebalanced 10 times in last hour
```

**Solutions:**
```properties
# Increase session timeout
session.timeout.ms=45000
heartbeat.interval.ms=3000

# Use static membership
group.instance.id=consumer-1

# Use cooperative sticky assignor
partition.assignment.strategy=CooperativeStickyAssignor
```

## Broker Performance Issues

### Symptom: High Broker CPU/Memory
```bash
# Check broker resource usage
kubectl top pod kafka-0

# High CPU/memory indicates overload
```

### Root Causes and Solutions

#### 1. Too Many Partitions
**Diagnosis:**
```bash
# Count total partitions
kafka-topics.sh --bootstrap-server localhost:9092 --describe \
  | grep "PartitionCount" | awk '{sum+=$2} END {print sum}'

# > 4000 partitions per broker is concerning
```

**Solutions:**
```bash
# Consolidate topics
# Reduce partition counts for low-traffic topics
# Add more brokers
```

#### 2. Large Message Sizes
**Diagnosis:**
```bash
# Check average message size
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic my-topic \
  --time -1

# Messages > 1MB need special handling
```

**Solutions:**
```properties
# Increase broker limits
message.max.bytes=10485760
replica.fetch.max.bytes=10485760

# Tune fetch sizes
fetch.max.bytes=52428800
```

#### 3. Insufficient Resources
**Diagnosis:**
```yaml
# Check resource allocation
resources:
  requests:
    memory: "2Gi"  # Too low for production
    cpu: "500m"    # Insufficient
```

**Solution:**
```yaml
resources:
  requests:
    memory: "8Gi"
    cpu: "4000m"
  limits:
    memory: "16Gi"
    cpu: "8000m"
```

## Network Connectivity Issues

### Symptom: Clients Cannot Connect
```logs
Error: Connection to node -1 could not be established
```

### Root Causes and Solutions

#### 1. Incorrect Advertised Listeners
**Diagnosis:**
```bash
# Check advertised listeners
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Verify advertised.listeners matches client network
```

**Solution:**
```properties
# Fix advertised listeners
advertised.listeners=INTERNAL://kafka-0:9092,EXTERNAL://external-ip:9094

# Ensure listeners match client access pattern
# Internal clients use INTERNAL
# External clients use EXTERNAL
```

#### 2. DNS Resolution Issues
**Diagnosis:**
```bash
# Test DNS resolution
nslookup kafka-0.kafka-headless.kafka.svc.cluster.local

# From external client
nslookup external-kafka-hostname
```

**Solution:**
```bash
# Fix DNS records
# Ensure LoadBalancer IPs are mapped correctly
# Update client bootstrap.servers with correct hostnames
```

#### 3. Network Policy Blocking
**Diagnosis:**
```bash
# Check network policies
kubectl get networkpolicy -n kafka

# Test connectivity
kubectl run -it --rm debug --image=busybox --restart=Never -- \
  nc -zv kafka-0 9092
```

**Solution:**
```yaml
# Update network policy to allow traffic
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kafka-allow
spec:
  podSelector:
    matchLabels:
      app: kafka
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: client-namespace
```

## Under-Replicated Partitions

### Symptom: Data At Risk
```bash
# Check under-replicated partitions
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions

# Output shows ISR < replicas
Topic: my-topic  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2
```

### Root Causes and Solutions

#### 1. Slow Replica
**Diagnosis:**
```bash
# Check replica lag time
# Monitor metric: kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
```

**Solutions:**
```properties
# Increase replica fetch timeout
replica.lag.time.max.ms=30000

# Increase replica fetch size
replica.fetch.max.bytes=1048576

# Check disk I/O on slow broker
iostat -x 1
```

#### 2. Network Partition
**Diagnosis:**
```bash
# Check broker connectivity
kafka-broker-api-versions.sh --bootstrap-server broker-ip:9092
```

**Solution:**
```bash
# Fix network issues
# Restart affected broker if needed
kubectl delete pod kafka-2
```

#### 3. Disk Full
**Diagnosis:**
```bash
# Check disk usage
kubectl exec kafka-0 -- df -h

# /var/lib/kafka/data at 100%
```

**Solution:**
```bash
# Clean old segments
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name my-topic \
  --alter --add-config retention.ms=86400000

# Or expand PVC
kubectl patch pvc kafka-data-kafka-0 \
  -p '{"spec":{"resources":{"requests":{"storage":"200Gi"}}}}'
```

## Message Loss or Duplication

### Symptom: Missing Messages
**Diagnosis:**
```bash
# Check producer acks setting
acks=1  # Potential data loss

# Check min.insync.replicas
min.insync.replicas=1  # Risky
```

**Solution:**
```properties
# Producer: Wait for all replicas
acks=all
enable.idempotence=true

# Broker: Require multiple replicas
min.insync.replicas=2
unclean.leader.election.enable=false
```

### Symptom: Duplicate Messages
**Diagnosis:**
```bash
# Check producer configuration
enable.idempotence=false  # Can cause duplicates
```

**Solution:**
```properties
# Enable idempotence
enable.idempotence=true
max.in.flight.requests.per.connection=5

# Consumer: Implement idempotent processing
# Use unique message IDs to detect duplicates
```

## Rebalancing Issues

### Symptom: Frequent Rebalancing
```logs
Group coordinator is rebalancing (0.1 rebalances/min)
```

### Root Causes and Solutions

#### 1. Long Processing Time
**Diagnosis:**
```properties
# Processing time > max.poll.interval.ms
max.poll.interval.ms=300000  # 5 minutes
# Processing takes 6 minutes
```

**Solution:**
```properties
# Increase timeout
max.poll.interval.ms=600000  # 10 minutes

# Or reduce batch size
max.poll.records=100
```

#### 2. Network Issues
**Diagnosis:**
```logs
Heartbeat failed, session timeout
```

**Solution:**
```properties
# Increase timeouts
session.timeout.ms=45000
heartbeat.interval.ms=3000
request.timeout.ms=40000
```

#### 3. Container Restarts
**Diagnosis:**
```bash
# Check pod restarts
kubectl get pods -n kafka

# Frequent restarts trigger rebalancing
```

**Solution:**
```properties
# Use static membership
group.instance.id=consumer-1

# Increase session timeout
session.timeout.ms=45000
```

## Controller Issues

### Symptom: No Active Controller
```bash
# Check controller
kafka-metadata.sh --snapshot /var/lib/kafka/metadata/__cluster_metadata-0/00000000000000000000.log \
  --print

# No active controller found
```

### Solutions

#### 1. Restart Controller Broker
```bash
# Identify controller broker
kubectl exec kafka-0 -- kafka-metadata.sh --snapshot \
  /var/lib/kafka/metadata/__cluster_metadata-0/00000000000000000000.log

# Restart that broker
kubectl delete pod kafka-<controller-id>
```

#### 2. Check Quorum
```bash
# For KRaft mode
# Ensure quorum has majority
# 3 controllers: need 2 alive
# 5 controllers: need 3 alive
```

## Disk Issues

### Symptom: Disk Full
```bash
# Check disk usage
kubectl exec kafka-0 -- df -h
# /var/lib/kafka/data 100%
```

### Solutions

#### 1. Reduce Retention
```bash
# Temporary: Reduce retention
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --alter \
  --add-config retention.ms=3600000
```

#### 2. Expand Storage
```bash
# Expand PVC (if storage class supports)
kubectl patch pvc data-kafka-0 \
  -p '{"spec":{"resources":{"requests":{"storage":"500Gi"}}}}'
```

#### 3. Delete Old Topics
```bash
# Delete unused topics
kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic old-topic
```

## Performance Degradation

### Symptom: Slow Throughput
**Diagnosis:**
```bash
# Producer throughput < expected
# Consumer lag increasing
```

### Diagnostic Commands
```bash
# Check producer performance
kafka-producer-perf-test.sh --topic test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092

# Check consumer performance
kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 \
  --topic test \
  --messages 1000000
```

### Solutions

#### 1. Tune Batching
```properties
# Producer
batch.size=65536
linger.ms=10
compression.type=lz4

# Consumer
fetch.min.bytes=1048576
max.poll.records=500
```

#### 2. Add Resources
```yaml
# Increase broker resources
resources:
  limits:
    cpu: "8000m"
    memory: "16Gi"
```

#### 3. Optimize JVM
```yaml
env:
- name: KAFKA_HEAP_OPTS
  value: "-Xms8g -Xmx8g"
- name: KAFKA_JVM_PERFORMANCE_OPTS
  value: "-XX:+UseG1GC -XX:MaxGCPauseMillis=20"
```

## Diagnostic Tools

### Essential Commands
```bash
# List all topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Describe topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --topic my-topic --describe

# Check consumer groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Describe consumer group
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --describe

# Check broker logs
kubectl logs kafka-0 -n kafka --tail=100

# Check metrics
kubectl port-forward kafka-0 7071:7071
curl localhost:7071/metrics
```

## Best Practices for Troubleshooting

1. **Monitor key metrics** continuously
2. **Enable debug logging** temporarily for issues
3. **Check broker logs** first for errors
4. **Verify network connectivity** between components
5. **Test with minimal configuration** to isolate issues
6. **Document incident timeline** for post-mortem
7. **Use staging environment** to reproduce issues
8. **Keep Kafka version updated** for bug fixes
9. **Implement proper monitoring** and alerting
10. **Regular health checks** and performance testing
