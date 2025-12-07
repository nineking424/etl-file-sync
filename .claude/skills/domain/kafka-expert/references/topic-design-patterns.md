# Topic Design Patterns

Comprehensive guide to designing Kafka topics for scalability, performance, and maintainability.

## Topic Naming Conventions

### Standard Naming Pattern
```
<domain>.<dataset>.<data-format>
```

**Examples:**
```
finance.transactions.avro
user.events.json
inventory.updates.protobuf
logs.application.plaintext
```

### Environment-Specific Naming
```
<env>.<domain>.<dataset>.<data-format>
```

**Examples:**
```
prod.finance.transactions.avro
dev.user.events.json
staging.inventory.updates.protobuf
```

### Best Practices
- Use lowercase letters, numbers, dots, underscores, and hyphens only
- Keep names under 249 characters
- Use descriptive, consistent naming
- Avoid generic names like "data" or "messages"
- Include version for breaking schema changes: `user.events.v2.avro`

## Partition Count Design

### Calculating Partition Count

**Formula:**
```
Partitions = max(
  Target Throughput / Producer Throughput per Partition,
  Target Throughput / Consumer Throughput per Partition
)
```

**Example Calculation:**
```
Target: 1M msgs/sec
Producer throughput: 10K msgs/sec per partition
Consumer throughput: 15K msgs/sec per partition

Producer partitions needed: 1M / 10K = 100
Consumer partitions needed: 1M / 15K = 67

Result: 100 partitions (use the higher value)
```

### Partition Count Guidelines

**Small Topics (< 10K msgs/sec):**
- Start with 6-12 partitions
- Allows room for growth
- Easy to manage and monitor

**Medium Topics (10K-100K msgs/sec):**
- Use 30-60 partitions
- Balance between parallelism and overhead
- Consider consumer group sizes

**Large Topics (> 100K msgs/sec):**
- Use 100+ partitions
- Monitor broker resource usage
- Consider partition reassignment overhead

**Factors to Consider:**
```yaml
# Partition count considerations
producer_throughput: 10000  # msgs/sec per partition
consumer_throughput: 15000  # msgs/sec per partition
max_consumers: 50           # maximum parallel consumers
broker_count: 10            # number of brokers
replication_factor: 3       # replication factor

# Constraints
# - Partitions should be multiple of broker count for even distribution
# - More partitions = more file handles and memory
# - Partition count cannot be decreased (only increased)
# - Each partition adds overhead for replication and leader election
```

## Retention Policies

### Time-Based Retention
```properties
# Delete data after 7 days
retention.ms=604800000

# Common retention periods
retention.ms=86400000      # 1 day
retention.ms=604800000     # 7 days
retention.ms=2592000000    # 30 days
retention.ms=31536000000   # 1 year
retention.ms=-1            # infinite
```

### Size-Based Retention
```properties
# Retain up to 100GB per partition
retention.bytes=107374182400

# Delete oldest segments when limit reached
# Useful for controlling storage costs
```

### Combined Retention
```properties
# Delete when either condition is met
retention.ms=604800000
retention.bytes=107374182400
# Data deleted when 7 days old OR 100GB reached
```

### Segment Configuration
```properties
# New segment every hour
segment.ms=3600000

# New segment at 1GB
segment.bytes=1073741824

# Affects:
# - Retention granularity (whole segments deleted)
# - Compaction efficiency
# - Recovery time
# - Number of files (too small = too many files)
```

### Log Compaction
```properties
# Enable compaction for changelog topics
cleanup.policy=compact

# Compaction settings
min.cleanable.dirty.ratio=0.5
delete.retention.ms=86400000
segment.ms=3600000

# Use cases:
# - Database changelogs
# - State snapshots
# - Configuration updates
# - User profile updates
```

## Topic Configuration Templates

### High-Throughput Events
```properties
# For high-volume event streams
compression.type=lz4
min.insync.replicas=2
retention.ms=604800000
segment.ms=3600000
max.message.bytes=1048576
message.timestamp.type=LogAppendTime
```

### Critical Financial Data
```properties
# For mission-critical data requiring durability
min.insync.replicas=3
unclean.leader.election.enable=false
retention.ms=31536000000
segment.ms=86400000
max.message.bytes=1048576
compression.type=none
```

### Log Aggregation
```properties
# For application logs
compression.type=snappy
min.insync.replicas=1
retention.ms=86400000
segment.ms=300000
max.message.bytes=10485760
cleanup.policy=delete
```

### CDC/Change Streams
```properties
# For database change data capture
cleanup.policy=compact
compression.type=snappy
min.insync.replicas=2
retention.ms=-1
segment.ms=3600000
delete.retention.ms=86400000
min.compaction.lag.ms=60000
```

## Multi-Topic Architectures

### Event Sourcing Pattern
```yaml
# Separate topics for different aggregates
topics:
  - orders.created.v1
  - orders.updated.v1
  - orders.cancelled.v1
  - inventory.reserved.v1
  - inventory.released.v1

# Each topic represents domain events
# Consumers rebuild state from events
```

### Stream Processing Pipeline
```yaml
# Input -> Processing -> Output pattern
raw-events:
  partitions: 50
  retention.ms: 86400000

enriched-events:
  partitions: 50
  retention.ms: 604800000

aggregated-metrics:
  partitions: 20
  retention.ms: 2592000000
  cleanup.policy: compact

# Data flows through transformation stages
```

### Dead Letter Queue Pattern
```yaml
# Main topic with DLQ for errors
user-events:
  partitions: 30
  retention.ms: 604800000

user-events-dlq:
  partitions: 10
  retention.ms: 2592000000

# Failed messages sent to DLQ for investigation
# DLQ has longer retention for debugging
```

## Topic Replication Design

### Replication Factor Guidelines
```yaml
# Production: RF = 3 (minimum)
replication.factor: 3
min.insync.replicas: 2

# Development: RF = 1 (acceptable)
replication.factor: 1
min.insync.replicas: 1

# Critical data: RF = 5 (maximum)
replication.factor: 5
min.insync.replicas: 3
```

### Rack Awareness
```properties
# Distribute replicas across failure domains
broker.rack=us-east-1a
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector

# Ensures replicas spread across racks/zones
# Improves availability during zone failures
```

## Performance Considerations

### Message Size Impact
```yaml
# Small messages (< 1KB)
# - High throughput potential
# - More messages per batch
# - Lower latency

# Large messages (> 100KB)
# - Configure max.message.bytes
# - May need larger fetch sizes
# - Consider object storage for very large payloads
```

### Batching Configuration
```properties
# Producer batching
batch.size=65536
linger.ms=10
compression.type=lz4

# More batching = better throughput, higher latency
# Less batching = lower latency, lower throughput
```

### Key-Based Partitioning
```java
// Partition by user ID for ordering guarantees
ProducerRecord<String, Event> record = new ProducerRecord<>(
    "user-events",
    userId,  // Partition key
    event
);

// Same user ID always goes to same partition
// Maintains order per user
// Consider cardinality: too few keys = hot partitions
```

## Best Practices Summary

1. **Start with fewer partitions** and increase as needed
2. **Use time-based retention** for most use cases
3. **Enable compression** to reduce storage and network costs
4. **Set min.insync.replicas** to at least 2 for important topics
5. **Use descriptive naming** for topics
6. **Document partition key strategy** for each topic
7. **Monitor partition skew** and rebalance if needed
8. **Plan for growth** but don't over-provision
9. **Use compaction** for changelog topics
10. **Test retention policies** with production-like data volumes
