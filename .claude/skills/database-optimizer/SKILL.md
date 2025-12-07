---
name: database-optimizer
description: Optimize database queries and configurations for Oracle and DuckDB including execution plan analysis, index design, and connection pool tuning
---

# Database Optimizer Skill

## Overview

The Database Optimizer skill provides expert guidance for optimizing database performance across Oracle and DuckDB. This skill covers execution plan analysis, index design strategies, batch processing optimization, connection pool tuning, and query performance troubleshooting.

## Core Capabilities

### 1. Execution Plan Analysis
- Interpret Oracle execution plans (EXPLAIN PLAN, DBMS_XPLAN)
- Analyze DuckDB query plans
- Identify performance bottlenecks (full table scans, nested loops)
- Optimize join orders and access paths
- Detect missing or unused indexes

### 2. Index Design and Recommendation
- Design B-tree indexes for selective queries
- Implement bitmap indexes for low-cardinality columns (Oracle)
- Create function-based indexes for computed columns
- Design composite indexes for multi-column predicates
- Analyze index usage and identify redundant indexes

### 3. JDBC Batch vs MyBatis ExecutorType.BATCH
- Compare raw JDBC batch processing performance
- Optimize MyBatis batch executor configuration
- Implement efficient bulk insert/update patterns
- Configure batch sizes for optimal throughput
- Handle batch errors and partial failures

### 4. Connection Pool Tuning
- Configure HikariCP for optimal performance
- Tune connection pool sizes (min, max)
- Set appropriate connection timeouts
- Monitor connection pool metrics
- Detect and resolve connection leaks

### 5. Query Optimization
- Rewrite queries for better performance
- Optimize WHERE clause predicates
- Improve JOIN strategies
- Reduce unnecessary data fetching
- Implement query result caching

## Workflow

When optimizing database performance:

### 1. Analysis Phase
- Identify slow queries using AWR/ASH reports (Oracle) or query logging
- Collect execution plans and statistics
- Analyze table and index statistics
- Review connection pool metrics
- Identify resource bottlenecks (CPU, I/O, memory)

### 2. Diagnosis
- Interpret execution plans for inefficiencies
- Identify missing or ineffective indexes
- Detect full table scans on large tables
- Analyze join methods and orders
- Review batch processing patterns

### 3. Optimization
- Create or modify indexes
- Rewrite queries for better performance
- Optimize batch processing configurations
- Tune connection pool parameters
- Update table statistics

### 4. Validation
- Re-run execution plans
- Compare before/after performance metrics
- Load test with production-like data
- Monitor resource utilization
- Validate query results remain correct

### 5. Monitoring
- Set up performance baselines
- Configure alerting for slow queries
- Monitor index usage over time
- Track connection pool metrics
- Review and optimize periodically

## Checklist

Before deploying optimizations:

- [ ] Execution plans show improved access paths
- [ ] Indexes are selective and used by queries
- [ ] Batch sizes are optimized for throughput
- [ ] Connection pool min/max sizes are appropriate
- [ ] Query timeout settings prevent long-running queries
- [ ] Statistics are up-to-date for optimizer decisions
- [ ] Load testing confirms performance improvements
- [ ] No regressions in other queries
- [ ] Monitoring is in place for new indexes
- [ ] Documentation updated with optimization rationale

## Usage Examples

### Example 1: Oracle Execution Plan Analysis
```sql
-- Get execution plan
EXPLAIN PLAN FOR
SELECT * FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date >= DATE '2024-01-01'
AND c.region = 'APAC';

-- Display plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- Identify issues:
-- - TABLE ACCESS FULL on orders (missing index on order_date)
-- - TABLE ACCESS FULL on customers (missing index on region)

-- Create indexes
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_customers_region ON customers(region);

-- Verify improvement
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
```

### Example 2: JDBC Batch Processing
```java
// JDBC Batch - Maximum performance
Connection conn = dataSource.getConnection();
conn.setAutoCommit(false);

String sql = "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)";
PreparedStatement stmt = conn.prepareStatement(sql);

int batchSize = 1000;
for (int i = 0; i < orders.size(); i++) {
    Order order = orders.get(i);
    stmt.setLong(1, order.getId());
    stmt.setLong(2, order.getCustomerId());
    stmt.setBigDecimal(3, order.getAmount());
    stmt.addBatch();

    if ((i + 1) % batchSize == 0) {
        stmt.executeBatch();
        conn.commit();
        stmt.clearBatch();
    }
}

// Execute remaining
if (orders.size() % batchSize != 0) {
    stmt.executeBatch();
    conn.commit();
}
```

### Example 3: MyBatis Batch Executor
```java
// MyBatis BATCH executor
SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH, false);
try {
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

    // Execute remaining
    if (orders.size() % batchSize != 0) {
        session.flushStatements();
        session.commit();
    }
} finally {
    session.close();
}
```

### Example 4: HikariCP Configuration
```properties
# Connection pool configuration
hikari.maximum-pool-size=20
hikari.minimum-idle=5
hikari.connection-timeout=30000
hikari.idle-timeout=600000
hikari.max-lifetime=1800000
hikari.leak-detection-threshold=60000

# Oracle-specific
hikari.connection-test-query=SELECT 1 FROM DUAL
hikari.data-source-properties.oracle.net.CONNECT_TIMEOUT=5000
hikari.data-source-properties.oracle.jdbc.ReadTimeout=60000
```

## Integration Points

### With Kafka Expert
- Optimize Kafka Connect JDBC source/sink connectors
- Tune batch sizes for Kafka to database writes
- Design CDC patterns with database triggers
- Optimize commit strategies for exactly-once delivery

### With ETL Pipeline Builder
- Optimize bulk data extraction queries
- Design efficient incremental load patterns
- Implement change data capture strategies
- Tune batch processing for ETL jobs

### With Airflow Architect
- Optimize database queries in Airflow tasks
- Configure connection pools for Airflow database backend
- Design efficient data validation queries
- Implement query caching for metadata operations

## References

See the `references/` directory for detailed guides on:
- **oracle-optimization.md**: Oracle-specific tuning, AWR analysis, and optimizer hints
- **duckdb-patterns.md**: DuckDB optimization, columnar storage, and parallel query execution
- **indexing-strategies.md**: Index types, selectivity analysis, and index maintenance
- **jdbc-batch-patterns.md**: Batch processing comparison and optimization techniques
- **connection-pool-tuning.md**: HikariCP configuration and connection management best practices

## Best Practices

1. **Always analyze execution plans** before creating indexes
2. **Update statistics** regularly for accurate optimizer decisions
3. **Use bind variables** to prevent SQL injection and improve plan caching
4. **Batch operations** for bulk insert/update/delete
5. **Monitor connection pool** metrics continuously
6. **Set appropriate timeouts** to prevent resource exhaustion
7. **Use read replicas** for reporting queries
8. **Implement query result caching** for frequently accessed data
9. **Partition large tables** for better query performance
10. **Regular index maintenance** (rebuild/reorganize as needed)
