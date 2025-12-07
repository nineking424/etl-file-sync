# JDBC Batch Patterns

Comparison and optimization of JDBC batch processing vs MyBatis BATCH executor.

## Raw JDBC Batch Processing

### Optimal Pattern
```java
Connection conn = dataSource.getConnection();
conn.setAutoCommit(false);

String sql = "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)";
PreparedStatement stmt = conn.prepareStatement(sql);

int batchSize = 1000;
int count = 0;

for (Order order : orders) {
    stmt.setLong(1, order.getId());
    stmt.setLong(2, order.getCustomerId());
    stmt.setBigDecimal(3, order.getAmount());
    stmt.addBatch();
    count++;

    if (count % batchSize == 0) {
        stmt.executeBatch();
        conn.commit();
        stmt.clearBatch();
    }
}

// Execute remaining
if (count % batchSize != 0) {
    stmt.executeBatch();
    conn.commit();
}

stmt.close();
conn.close();
```

## MyBatis BATCH Executor

### Configuration
```java
SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH, false);
OrderMapper mapper = session.getMapper(OrderMapper.class);

int batchSize = 1000;
for (int i = 0; i < orders.size(); i++) {
    mapper.insert(orders.get(i));

    if ((i + 1) % batchSize == 0) {
        session.flushStatements();
        session.commit();
        session.clearCache();
    }
}

session.flushStatements();
session.commit();
session.close();
```

## Performance Comparison

### Benchmark Results
```
Test: 100,000 inserts

JDBC Batch (1000):     2.5 seconds
MyBatis BATCH (1000):  3.1 seconds
MyBatis Simple:        45.2 seconds
No Batch:              180.4 seconds

Conclusion: JDBC Batch ~20% faster than MyBatis BATCH
           MyBatis BATCH ~14x faster than Simple executor
```

## Batch Size Tuning

### Optimal Batch Size
```java
// Test different batch sizes
int[] batchSizes = {100, 500, 1000, 5000, 10000};

for (int batchSize : batchSizes) {
    long start = System.currentTimeMillis();
    insertWithBatch(orders, batchSize);
    long duration = System.currentTimeMillis() - start;
    System.out.println("Batch size " + batchSize + ": " + duration + "ms");
}

// Typical optimal: 500-2000 rows per batch
```

## Error Handling

### Batch with Error Recovery
```java
try {
    stmt.executeBatch();
    conn.commit();
} catch (BatchUpdateException e) {
    int[] updateCounts = e.getUpdateCounts();
    for (int i = 0; i < updateCounts.length; i++) {
        if (updateCounts[i] == Statement.EXECUTE_FAILED) {
            // Handle failed row
            handleFailedRow(orders.get(i));
        }
    }
    conn.rollback();
}
```

## Best Practices

1. **Use batch size 500-2000** for optimal performance
2. **Disable auto-commit** during batch operations
3. **Commit periodically** to avoid long transactions
4. **Clear batch** after execution
5. **Handle BatchUpdateException** properly
6. **Use JDBC batch** for maximum performance
7. **Use MyBatis BATCH** for better code maintainability
8. **Monitor memory usage** with large batches
9. **Test with production data volumes**
10. **Use connection pooling** (HikariCP)
