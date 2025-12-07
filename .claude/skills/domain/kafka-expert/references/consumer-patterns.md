# Consumer Patterns

Comprehensive guide to Kafka consumer group design, offset management, and consumption patterns.

## Consumer Group Design

### Basic Consumer Group
```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "order-processing-group");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("enable.auto.commit", "false");
props.put("auto.offset.reset", "earliest");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("orders"));
```

### Multiple Consumer Groups Pattern
```yaml
# Different consumer groups for different purposes
order-processing-group:
  purpose: Process orders in real-time
  consumers: 10
  partitions_per_consumer: 3

order-analytics-group:
  purpose: Analytics and reporting
  consumers: 5
  partitions_per_consumer: 6

order-audit-group:
  purpose: Audit logging
  consumers: 2
  partitions_per_consumer: 15

# Each group maintains independent offsets
# Can process same data at different rates
```

### Fan-Out Pattern
```java
// Multiple independent consumer groups read same topic
// Each group processes all messages independently

// Real-time processor
Consumer group: realtime-processor
Topics: user-events
Purpose: Immediate event processing

// Batch processor
Consumer group: batch-processor
Topics: user-events
Purpose: Hourly batch aggregation

// Archive processor
Consumer group: archive-processor
Topics: user-events
Purpose: Long-term storage
```

## Offset Management Strategies

### Manual Commit (Synchronous)
```java
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    for (ConsumerRecord<String, String> record : records) {
        try {
            // Process record
            processRecord(record);

            // Commit after each record (slow but safe)
            consumer.commitSync(Collections.singletonMap(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
            ));
        } catch (Exception e) {
            // Handle error and don't commit
            log.error("Failed to process record", e);
        }
    }
}
```

### Manual Commit (Asynchronous)
```java
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    for (ConsumerRecord<String, String> record : records) {
        processRecord(record);
    }

    // Commit batch asynchronously
    consumer.commitAsync((offsets, exception) -> {
        if (exception != null) {
            log.error("Commit failed for offsets: {}", offsets, exception);
            // Implement retry or alert logic
        }
    });
}
```

### At-Least-Once Processing
```java
// Commit after processing
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    for (ConsumerRecord<String, String> record : records) {
        // Process first
        processRecord(record);

        // Then commit
        consumer.commitSync();
    }
}

// If crash occurs after processing but before commit:
// - Record will be reprocessed
// - Make processing idempotent
```

### At-Most-Once Processing
```java
// Commit before processing (rarely used)
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    // Commit first
    consumer.commitSync();

    // Then process
    for (ConsumerRecord<String, String> record : records) {
        try {
            processRecord(record);
        } catch (Exception e) {
            // Data loss: record won't be reprocessed
            log.error("Lost record: {}", record, e);
        }
    }
}
```

### Exactly-Once Processing
```java
// Using transactions
Properties props = new Properties();
props.put("enable.idempotence", "true");
props.put("transactional.id", "order-processor-1");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.initTransactions();

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    producer.beginTransaction();
    try {
        for (ConsumerRecord<String, String> record : records) {
            // Process and produce results
            String result = processRecord(record);
            producer.send(new ProducerRecord<>("results", result));
        }

        // Commit offsets as part of transaction
        Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(records);
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

        producer.commitTransaction();
    } catch (Exception e) {
        producer.abortTransaction();
    }
}
```

## Rebalancing Strategies

### Cooperative Sticky Assignment
```properties
# Recommended for production
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor

# Benefits:
# - Incremental rebalancing
# - Reduced stop-the-world pauses
# - Better for large consumer groups
```

### Static Membership
```properties
# Prevent rebalancing on short restarts
group.instance.id=consumer-1
session.timeout.ms=45000

# Benefits:
# - Faster restarts without rebalancing
# - Maintains partition assignments
# - Useful for containerized deployments
```

### Handling Rebalance Events
```java
consumer.subscribe(Arrays.asList("orders"), new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // Called before rebalancing
        // Commit offsets for partitions being revoked
        consumer.commitSync();
        log.info("Partitions revoked: {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Called after rebalancing
        // Initialize state for new partitions
        log.info("Partitions assigned: {}", partitions);
        for (TopicPartition partition : partitions) {
            // Seek to specific offset if needed
            // consumer.seek(partition, getLastProcessedOffset(partition));
        }
    }
});
```

## Consumer Lag Management

### Monitoring Lag
```bash
# Check consumer group lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --describe

# Output shows:
# GROUP    TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# my-group my-topic   0          100             150             50
```

### Handling High Lag

#### 1. Scale Out Consumers
```java
// Add more consumer instances
// Up to partition count limit

// Before: 2 consumers, 10 partitions = 5 partitions each
// After: 5 consumers, 10 partitions = 2 partitions each
```

#### 2. Increase Fetch Size
```properties
# Fetch more data per poll
fetch.min.bytes=1048576
fetch.max.bytes=52428800
max.partition.fetch.bytes=1048576

# Process more records per batch
max.poll.records=1000
```

#### 3. Parallel Processing
```java
ExecutorService executor = Executors.newFixedThreadPool(10);

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    List<Future<?>> futures = new ArrayList<>();
    for (ConsumerRecord<String, String> record : records) {
        futures.add(executor.submit(() -> processRecord(record)));
    }

    // Wait for all processing to complete
    for (Future<?> future : futures) {
        future.get();
    }

    consumer.commitSync();
}
```

#### 4. Skip Lag (Last Resort)
```java
// Seek to end of partition
consumer.poll(Duration.ofMillis(0)); // Initialize assignment
consumer.seekToEnd(consumer.assignment());
consumer.commitSync();

// Warning: This discards data!
// Only use for non-critical catch-up scenarios
```

## Advanced Consumer Patterns

### Pausing and Resuming
```java
// Pause consumption for backpressure control
Set<TopicPartition> assignedPartitions = consumer.assignment();

if (isDownstreamOverloaded()) {
    consumer.pause(assignedPartitions);
    log.info("Paused consumption");
}

// Resume when ready
if (canResumeProcessing()) {
    consumer.resume(assignedPartitions);
    log.info("Resumed consumption");
}
```

### Seeking to Specific Offset
```java
// Seek to beginning
consumer.seekToBeginning(consumer.assignment());

// Seek to end
consumer.seekToEnd(consumer.assignment());

// Seek to specific offset
TopicPartition partition = new TopicPartition("orders", 0);
consumer.seek(partition, 100L);

// Seek to timestamp
Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
timestampsToSearch.put(partition, System.currentTimeMillis() - 3600000);
Map<TopicPartition, OffsetAndTimestamp> offsets =
    consumer.offsetsForTimes(timestampsToSearch);
```

### Multi-Topic Consumer
```java
// Subscribe to multiple topics
consumer.subscribe(Arrays.asList("orders", "payments", "shipments"));

// Pattern-based subscription
consumer.subscribe(Pattern.compile("order-.*"));

// Process records from different topics
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    for (ConsumerRecord<String, String> record : records) {
        switch (record.topic()) {
            case "orders":
                processOrder(record);
                break;
            case "payments":
                processPayment(record);
                break;
            case "shipments":
                processShipment(record);
                break;
        }
    }
}
```

## Error Handling Strategies

### Dead Letter Queue
```java
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    for (ConsumerRecord<String, String> record : records) {
        try {
            processRecord(record);
        } catch (RecoverableException e) {
            // Retry logic
            retryRecord(record);
        } catch (UnrecoverableException e) {
            // Send to DLQ
            sendToDLQ(record, e);
        }
    }

    consumer.commitSync();
}

private void sendToDLQ(ConsumerRecord<String, String> record, Exception e) {
    ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
        record.topic() + "-dlq",
        record.key(),
        record.value()
    );
    dlqRecord.headers().add("error", e.getMessage().getBytes());
    dlqRecord.headers().add("original-partition",
        String.valueOf(record.partition()).getBytes());
    dlqRecord.headers().add("original-offset",
        String.valueOf(record.offset()).getBytes());

    dlqProducer.send(dlqRecord);
}
```

### Retry with Backoff
```java
private void processWithRetry(ConsumerRecord<String, String> record) {
    int maxRetries = 3;
    int retryDelayMs = 1000;

    for (int i = 0; i < maxRetries; i++) {
        try {
            processRecord(record);
            return; // Success
        } catch (Exception e) {
            if (i == maxRetries - 1) {
                sendToDLQ(record, e);
            } else {
                Thread.sleep(retryDelayMs * (i + 1));
            }
        }
    }
}
```

## Performance Tuning

### Optimal Configuration
```properties
# Batch processing
fetch.min.bytes=1048576
max.poll.records=500
max.partition.fetch.bytes=1048576

# Connection settings
session.timeout.ms=30000
heartbeat.interval.ms=3000
max.poll.interval.ms=300000

# Network settings
receive.buffer.bytes=65536
send.buffer.bytes=131072
```

### Metrics to Monitor
```yaml
metrics:
  - consumer-lag: Current lag per partition
  - records-consumed-rate: Records/sec
  - fetch-latency-avg: Average fetch latency
  - commit-latency-avg: Average commit latency
  - join-rate: Rebalances per hour
  - heartbeat-rate: Heartbeats per second
```

## Best Practices

1. **Use manual commit** for critical data processing
2. **Implement idempotent processing** for at-least-once semantics
3. **Monitor consumer lag** as primary health metric
4. **Use static membership** for stable deployments
5. **Handle rebalancing gracefully** with listeners
6. **Set appropriate timeouts** based on processing time
7. **Implement circuit breakers** for downstream failures
8. **Use cooperative sticky assignor** for large groups
9. **Process in batches** for better throughput
10. **Always close consumers** properly on shutdown
