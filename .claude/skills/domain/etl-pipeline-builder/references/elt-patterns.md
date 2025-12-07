# ELT Patterns

Extract-Load-Transform patterns for modern data warehouses.

## ELT vs ETL

### When to Use ELT
- Modern data warehouses (Snowflake, BigQuery, Redshift)
- Large data volumes
- Complex transformations benefit from warehouse compute
- Need to preserve raw data
- Multiple transformation use cases

### When to Use ETL
- Legacy databases without strong compute
- Network bandwidth constraints
- Need to filter/reduce data volume
- Sensitive data requiring masking before load
- Simple transformations

## Raw Data Ingestion

### Load Raw Data
```python
def load_raw_data(source_table, target_warehouse):
    """Load data as-is to raw layer"""
    
    # Extract with minimal transformation
    data = extract_full_table(source_table)
    
    # Load to raw zone with metadata
    for row in data:
        row['_extracted_at'] = datetime.now()
        row['_source_system'] = 'erp_prod'
    
    # Bulk load to warehouse
    bulk_insert(target_warehouse, f"raw_{source_table}", data)
```

## In-Warehouse Transformations

### Staging Layer
```sql
-- Create staging view with basic transformations
CREATE OR REPLACE VIEW staging.orders AS
SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) as order_date,
    CAST(amount AS DECIMAL(10,2)) as amount,
    UPPER(TRIM(status)) as status,
    _extracted_at
FROM raw.orders
WHERE _extracted_at >= CURRENT_DATE - INTERVAL '7 days';
```

### Fact Table Transformation
```sql
-- Transform to fact table
CREATE OR REPLACE TABLE warehouse.fact_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    c.customer_segment,
    c.customer_region,
    p.product_category,
    CURRENT_TIMESTAMP as processed_at
FROM staging.orders o
JOIN staging.customers c ON o.customer_id = c.customer_id
JOIN staging.products p ON o.product_id = p.product_id;
```

### Aggregation Layer
```sql
-- Create aggregated metrics
CREATE OR REPLACE TABLE warehouse.daily_metrics AS
SELECT
    order_date,
    customer_segment,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT customer_id) as unique_customers
FROM warehouse.fact_orders
GROUP BY order_date, customer_segment;
```

## Incremental ELT

### Incremental Raw Load
```python
def incremental_raw_load():
    last_watermark = get_watermark("orders")
    
    # Extract incremental
    query = f"""
        SELECT *, CURRENT_TIMESTAMP as _extracted_at
        FROM orders 
        WHERE updated_at > :watermark
    """
    data = execute_query(query, watermark=last_watermark)
    
    # Append to raw table
    append_to_table("raw.orders", data)
    
    # Update watermark
    update_watermark("orders", datetime.now())
```

### Incremental Transformation
```sql
-- Incrementally update fact table
MERGE INTO warehouse.fact_orders t
USING (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.amount,
        c.customer_segment
    FROM staging.orders o
    JOIN staging.customers c ON o.customer_id = c.customer_id
    WHERE o._extracted_at >= CURRENT_DATE
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    t.amount = s.amount,
    t.customer_segment = s.customer_segment,
    t.processed_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT VALUES (
    s.order_id, s.customer_id, s.order_date, 
    s.amount, s.customer_segment, CURRENT_TIMESTAMP
);
```

## dbt for ELT

### dbt Model Example
```sql
-- models/staging/stg_orders.sql
{{
    config(
        materialized='view'
    )
}}

SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) as order_date,
    CAST(amount AS DECIMAL(10,2)) as amount,
    UPPER(TRIM(status)) as status
FROM {{ source('raw', 'orders') }}
WHERE _extracted_at >= CURRENT_DATE - INTERVAL '7 days'
```

```sql
-- models/warehouse/fact_orders.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    c.customer_segment
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c 
  ON o.customer_id = c.customer_id

{% if is_incremental() %}
WHERE o.order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

## Best Practices

1. **Preserve raw data** for reprocessing
2. **Use views** for staging transformations
3. **Leverage warehouse compute** for heavy transformations
4. **Implement layered architecture** (raw/staging/warehouse)
5. **Use dbt** for transformation management
6. **Partition large tables** by date
7. **Implement incremental loads**
8. **Monitor transformation performance**
9. **Version transformations** with dbt/git
10. **Test transformations** before production
