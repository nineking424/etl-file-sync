# Indexing Strategies

Comprehensive guide to database index design and optimization.

## Index Types

### B-Tree Index (Default)
```sql
-- Best for: High cardinality columns, range queries
CREATE INDEX idx_orders_date ON orders(order_date);

-- Good for:
-- - WHERE order_date >= '2024-01-01'
-- - ORDER BY order_date
-- - Range scans
```

### Composite Index
```sql
-- Order matters! Leading column most selective
CREATE INDEX idx_orders_date_status ON orders(order_date, status);

-- Covers:
-- - WHERE order_date = X AND status = Y
-- - WHERE order_date = X
-- Not covered:
-- - WHERE status = Y (leading column not used)
```

### Covering Index
```sql
-- Include all columns needed by query
CREATE INDEX idx_orders_covering 
ON orders(customer_id) 
INCLUDE (order_date, amount);

-- Query doesn't need to access table
SELECT customer_id, order_date, amount
FROM orders WHERE customer_id = 1000;
```

## Index Selectivity

### Calculate Selectivity
```sql
-- Selectivity = distinct values / total rows
SELECT 
    COUNT(DISTINCT customer_id) / COUNT(*) as selectivity
FROM orders;

-- Good index: selectivity > 0.01 (1%)
-- Poor index: selectivity < 0.001 (0.1%)
```

## Index Maintenance

### Rebuild Index
```sql
-- Oracle
ALTER INDEX idx_orders_date REBUILD ONLINE;

-- Check fragmentation
SELECT index_name, blevel, leaf_blocks
FROM user_indexes
WHERE index_name = 'IDX_ORDERS_DATE';
```

### Drop Unused Indexes
```sql
-- Find unused indexes (Oracle)
SELECT index_name, table_name
FROM user_indexes
WHERE index_name NOT IN (
    SELECT object_name FROM v$sql_plan
    WHERE object_type = 'INDEX'
);
```

## Best Practices

1. **Index foreign keys** for join performance
2. **Create composite indexes** for multi-column predicates
3. **Leading column** should be most selective
4. **Monitor index usage** and drop unused
5. **Rebuild fragmented indexes** periodically
6. **Use covering indexes** for frequently accessed columns
7. **Avoid over-indexing** (impacts write performance)
8. **Update statistics** after index creation
9. **Test index effectiveness** with EXPLAIN PLAN
10. **Consider partial indexes** for filtered queries
