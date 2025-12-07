# Partition Strategies

Detailed guide to partition key selection, data distribution, and ordering guarantees in Apache Kafka.

## Partition Key Selection

### Key-Based Partitioning
```java
// Default partitioner uses key hash
ProducerRecord<String, String> record = new ProducerRecord<>(
    "topic-name",
    "user-123",  // Key determines partition
    "payload"
);

// Partition = hash(key) % partition_count
// Same key always goes to same partition
```

### Custom Partitioner
```java
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        // Custom logic for partition assignment
        if (key instanceof String) {
            String keyStr = (String) key;
            if (keyStr.startsWith("premium-")) {
                // Premium users to first 10% of partitions
                return Math.abs(keyStr.hashCode()) % (numPartitions / 10);
            }
        }

        // Default for other keys
        return Math.abs(keyBytes.hashCode()) % numPartitions;
    }
}
```

### Round-Robin Partitioning
```java
// Null key triggers round-robin
ProducerRecord<String, String> record = new ProducerRecord<>(
    "topic-name",
    null,  // Null key = round-robin
    "payload"
);

// Good for: Load balancing when order doesn't matter
// Bad for: When you need ordering guarantees
```

## Partition Key Patterns

### User-Based Partitioning
```java
// Partition by user ID
String key = userId;
// Pros: Ordered events per user, easy to debug
// Cons: Risk of hot partitions if user activity varies widely

// Example: Social media events
ProducerRecord<String, UserEvent> record = new ProducerRecord<>(
    "user-events",
    event.getUserId(),  // All events for user in same partition
    event
);
```

### Tenant-Based Partitioning
```java
// Multi-tenant applications
String key = tenantId;
// Pros: Tenant isolation, easy per-tenant replay
// Cons: Large tenants create hot partitions

// Example: SaaS application events
ProducerRecord<String, TenantEvent> record = new ProducerRecord<>(
    "tenant-events",
    event.getTenantId(),
    event
);
```

### Time-Window Partitioning
```java
// Partition by time window
String key = String.format("%s-%s",
    entityId,
    Instant.now().truncatedTo(ChronoUnit.HOURS)
);

// Pros: Time-based data locality
// Cons: All consumers process different time windows

// Example: Metrics aggregation
ProducerRecord<String, Metric> record = new ProducerRecord<>(
    "metrics",
    String.format("%s-%d", metric.getName(), hourTimestamp),
    metric
);
```

### Composite Key Partitioning
```java
// Multiple dimensions in key
String key = String.format("%s|%s", region, customerId);

// Pros: Better distribution than single dimension
// Cons: More complex consumer logic

// Example: Regional customer data
ProducerRecord<String, Order> record = new ProducerRecord<>(
    "orders",
    String.format("%s|%s", order.getRegion(), order.getCustomerId()),
    order
);
```

## Data Distribution Strategies

### Even Distribution
```java
// Hash-based distribution for even load
Properties props = new Properties();
props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");

// When to use:
// - No ordering requirements
// - Want to maximize throughput
// - Avoid hot partitions
```

### Controlled Skew
```java
// Deliberately skew for prioritization
public class PriorityPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);

        // Parse priority from key
        String keyStr = (String) key;
        String[] parts = keyStr.split(":");
        String priority = parts[0];

        if ("HIGH".equals(priority)) {
            // High priority to first 20% of partitions
            return Math.abs(parts[1].hashCode()) % (numPartitions / 5);
        } else if ("MEDIUM".equals(priority)) {
            // Medium priority to next 30% of partitions
            int offset = numPartitions / 5;
            int range = (numPartitions * 3) / 10;
            return offset + (Math.abs(parts[1].hashCode()) % range);
        } else {
            // Low priority to remaining 50% of partitions
            int offset = (numPartitions / 5) + ((numPartitions * 3) / 10);
            int range = numPartitions / 2;
            return offset + (Math.abs(parts[1].hashCode()) % range);
        }
    }
}
```

### Sticky Partitioning
```properties
# Enable sticky partitioning for better batching
partitioner.class=org.apache.kafka.clients.producer.internals.DefaultPartitioner
partitioner.ignore.keys=true

# Batches messages to same partition for efficiency
# Good for high-throughput scenarios without ordering needs
```

## Ordering Guarantees

### Per-Partition Ordering
```java
// Kafka guarantees order within a partition
// Messages with same key maintain order

// Example: User activity timeline
for (Activity activity : userActivities) {
    ProducerRecord<String, Activity> record = new ProducerRecord<>(
        "user-activities",
        activity.getUserId(),  // Same user = same partition
        activity
    );
    producer.send(record);
}

// Consumer sees activities in order for each user
```

### Global Ordering
```properties
# Single partition = global order
num.partitions=1

# Pros: Total ordering
# Cons: No parallelism, low throughput
# Use only when absolutely necessary
```

### Ordering with Idempotence
```properties
# Enable idempotence for guaranteed ordering
enable.idempotence=true
max.in.flight.requests.per.connection=5
acks=all
retries=Integer.MAX_VALUE

# Prevents reordering during retries
# Safe for production with ordering requirements
```

## Hot Partition Mitigation

### Detecting Hot Partitions
```bash
# Monitor partition metrics
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --describe

# Look for:
# - High lag on specific partitions
# - Uneven consumer processing times
# - Partition size differences
```

### Strategies to Avoid Hot Partitions

#### 1. Add Randomness to Key
```java
// Add random suffix to high-cardinality keys
String key = String.format("%s-%d", userId, random.nextInt(10));

// Distributes single user across 10 partitions
// Trade-off: Loses strict ordering per user
```

#### 2. Use Composite Keys
```java
// Combine multiple attributes
String key = String.format("%s|%s|%d",
    region,
    userId,
    timestamp % 100
);

// Better distribution while maintaining logical grouping
```

#### 3. Increase Partition Count
```bash
# Add partitions to existing topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --alter --topic my-topic \
  --partitions 100

# Note: Existing messages stay in original partitions
# New messages distributed across all partitions
```

#### 4. Split Hot Keys
```java
// Separate high-volume keys to dedicated topic
if (isHighVolumeUser(userId)) {
    producer.send(new ProducerRecord<>("high-volume-users", userId, event));
} else {
    producer.send(new ProducerRecord<>("regular-users", userId, event));
}
```

## Partition Reassignment

### Manual Reassignment
```json
// reassignment.json
{
  "version": 1,
  "partitions": [
    {
      "topic": "my-topic",
      "partition": 0,
      "replicas": [1, 2, 3],
      "log_dirs": ["any", "any", "any"]
    }
  ]
}
```

```bash
# Execute reassignment
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassignment.json \
  --execute

# Monitor progress
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassignment.json \
  --verify
```

### Automated Rebalancing with Cruise Control
```yaml
# Cruise Control goals
- RackAwareGoal
- ReplicaCapacityGoal
- DiskCapacityGoal
- NetworkInboundCapacityGoal
- NetworkOutboundCapacityGoal
- CpuCapacityGoal
- ReplicaDistributionGoal
- LeaderReplicaDistributionGoal
- LeaderBytesInDistributionGoal

# Automatically rebalances based on metrics
```

## Testing Partition Strategies

### Distribution Analysis
```python
# Python script to analyze key distribution
from collections import defaultdict
import mmh3  # MurmurHash3

def analyze_distribution(keys, num_partitions):
    partition_counts = defaultdict(int)

    for key in keys:
        # Simulate Kafka's default partitioner
        partition = (mmh3.hash(key) & 0x7fffffff) % num_partitions
        partition_counts[partition] += 1

    # Calculate statistics
    avg = len(keys) / num_partitions
    max_count = max(partition_counts.values())
    min_count = min(partition_counts.values())

    print(f"Average: {avg}")
    print(f"Max: {max_count} ({max_count/avg:.2%} of average)")
    print(f"Min: {min_count} ({min_count/avg:.2%} of average)")
    print(f"Standard deviation: {std_dev(partition_counts.values())}")

    return partition_counts
```

### Load Testing
```java
// Generate representative workload
public class PartitionLoadTest {
    public static void main(String[] args) {
        // Simulate realistic key distribution
        Map<String, Integer> keyFrequencies = new HashMap<>();

        // Zipf distribution for realistic workload
        for (int i = 0; i < 1000000; i++) {
            String key = generateZipfKey(i);
            keyFrequencies.merge(key, 1, Integer::sum);
        }

        // Send to Kafka and measure partition distribution
        analyzePartitionDistribution(keyFrequencies);
    }
}
```

## Best Practices

1. **Choose keys with high cardinality** to avoid hot partitions
2. **Test distribution** with production-like data before deployment
3. **Monitor partition metrics** continuously
4. **Document key strategy** for operational teams
5. **Consider future growth** when selecting partition count
6. **Use composite keys** for better distribution
7. **Enable idempotence** when ordering matters
8. **Avoid over-partitioning** (impacts latency and resources)
9. **Plan for rebalancing** as data patterns change
10. **Use custom partitioners** for special requirements

## Common Pitfalls

- **Using low-cardinality keys** (e.g., boolean flags)
- **Not considering data skew** in key distribution
- **Decreasing partition count** (not supported)
- **Ignoring hot partitions** until they cause outages
- **Complex partition logic** that's hard to debug
- **Not testing with production data volumes**
- **Forgetting about consumer parallelism limits**
