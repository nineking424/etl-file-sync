# DuckDB Patterns

DuckDB-specific optimization patterns and columnar storage best practices.

## Query Optimization

### Columnar Storage Benefits
```sql
-- DuckDB automatically uses columnar storage
-- Excellent for analytical queries

-- Efficient column projection
SELECT customer_id, SUM(amount) FROM orders
GROUP BY customer_id;

-- Only reads customer_id and amount columns
-- Skips unused columns entirely
```

### Parallel Query Execution
```sql
-- DuckDB automatically parallelizes queries
-- Configure thread count
SET threads = 8;

-- Check parallelization
EXPLAIN SELECT COUNT(*) FROM large_table;
```

## Data Import Patterns

### Parquet Import
```sql
-- Read Parquet files directly
SELECT * FROM 'data/*.parquet';

-- Create table from Parquet
CREATE TABLE orders AS 
SELECT * FROM 'orders.parquet';
```

### CSV Import
```sql
-- Read CSV efficiently
COPY orders FROM 'orders.csv' (HEADER TRUE, DELIMITER ',');
```

## Aggregation Optimization

### Window Functions
```sql
-- Efficient window function
SELECT 
    order_id,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as row_num
FROM orders;
```

## Best Practices

1. **Use Parquet** for analytical workloads
2. **Enable parallelism** with appropriate thread count
3. **Use columnar projections** (select only needed columns)
4. **Leverage vectorized execution**
5. **Use appropriate data types**
6. **Partition large datasets** by query patterns
7. **Use approximate aggregates** for large datasets
8. **Implement pushdown filters**
9. **Monitor memory usage**
10. **Use batch inserts** for loading data
