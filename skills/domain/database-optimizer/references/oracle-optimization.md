# Oracle Optimization

Guide to Oracle-specific performance tuning and optimization techniques.

## Execution Plan Analysis

### Basic Execution Plan
```sql
EXPLAIN PLAN FOR
SELECT * FROM orders WHERE order_date >= DATE '2024-01-01';

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- Look for:
-- - TABLE ACCESS FULL (inefficient on large tables)
-- - INDEX RANGE SCAN (good for selective queries)
-- - NESTED LOOPS vs HASH JOIN
-- - Sort operations (can be expensive)
```

### Advanced Plan Analysis
```sql
-- Get actual execution statistics
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST'));

-- Analyze with runtime statistics
SET AUTOTRACE ON
SELECT * FROM orders WHERE customer_id = 1000;
SET AUTOTRACE OFF
```

## AWR Report Analysis

### Generate AWR Report
```sql
-- List available snapshots
SELECT snap_id, begin_interval_time, end_interval_time
FROM dba_hist_snapshot
ORDER BY snap_id DESC
FETCH FIRST 20 ROWS ONLY;

-- Generate AWR report
@?/rdbms/admin/awrrpt.sql
-- Enter begin and end snapshot IDs
```

### Key AWR Metrics
- **Top SQL**: Identify high-resource queries
- **Wait Events**: I/O, latch, lock waits
- **Instance Efficiency**: Buffer cache hit ratio, parse ratio
- **Load Profile**: Transactions/sec, logical reads/sec

## Optimizer Hints

### Common Hints
```sql
-- Force index usage
SELECT /*+ INDEX(orders idx_order_date) */
FROM orders WHERE order_date >= DATE '2024-01-01';

-- Force full table scan
SELECT /*+ FULL(orders) */
FROM orders WHERE customer_id = 1000;

-- Choose join method
SELECT /*+ USE_HASH(o c) */
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- Parallel execution
SELECT /*+ PARALLEL(orders, 8) */
FROM orders WHERE order_date >= DATE '2024-01-01';
```

## Index Design

### B-Tree Index
```sql
-- Standard index for high selectivity
CREATE INDEX idx_orders_customer ON orders(customer_id);

-- Composite index
CREATE INDEX idx_orders_date_status ON orders(order_date, status);
```

### Bitmap Index
```sql
-- For low cardinality columns
CREATE BITMAP INDEX idx_orders_status ON orders(status);
-- Good for data warehouse, not OLTP
```

### Function-Based Index
```sql
-- Index on expression
CREATE INDEX idx_orders_upper_name ON orders(UPPER(customer_name));

SELECT * FROM orders WHERE UPPER(customer_name) = 'JOHN DOE';
```

## Statistics Management

### Gather Statistics
```sql
-- Table statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS('SCHEMA', 'ORDERS');

-- Schema statistics
EXEC DBMS_STATS.GATHER_SCHEMA_STATS('SCHEMA', CASCADE => TRUE);

-- Auto gather (recommended)
EXEC DBMS_STATS.SET_GLOBAL_PREFS('AUTO_STATS_COLLECTION', 'TRUE');
```

## Partitioning

### Range Partitioning
```sql
CREATE TABLE orders (
    order_id NUMBER,
    order_date DATE,
    amount NUMBER
)
PARTITION BY RANGE (order_date) (
    PARTITION p_2023 VALUES LESS THAN (DATE '2024-01-01'),
    PARTITION p_2024 VALUES LESS THAN (DATE '2025-01-01')
);

-- Query specific partition
SELECT * FROM orders PARTITION (p_2024)
WHERE order_date >= DATE '2024-01-01';
```

## Best Practices

1. **Keep statistics current** with auto gather
2. **Use bind variables** to enable plan reuse
3. **Partition large tables** for manageability
4. **Monitor AWR reports** regularly
5. **Use hints sparingly** (let optimizer decide)
6. **Implement proper indexing** strategy
7. **Enable query result cache** for read-heavy workloads
8. **Use connection pooling** (HikariCP)
9. **Regular index maintenance** (rebuild/reorganize)
10. **Monitor wait events** for bottlenecks
