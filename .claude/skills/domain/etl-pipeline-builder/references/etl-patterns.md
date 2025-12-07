# ETL Patterns

Extract-Transform-Load design patterns and best practices.

## Full Load Pattern

### Complete Table Replacement
```python
def full_load_etl():
    # Extract
    data = extract_full_table("source_db", "orders")
    
    # Transform
    transformed = transform_data(data)
    
    # Load with truncate-insert
    truncate_table("target_db", "orders")
    bulk_insert("target_db", "orders", transformed)
```

## Incremental Load Pattern

### Timestamp-Based Incremental
```python
def incremental_load_etl():
    # Get last watermark
    last_updated = get_watermark("orders_watermark")
    
    # Extract incremental data
    query = f"""
        SELECT * FROM orders 
        WHERE updated_at > :watermark 
        ORDER BY updated_at
    """
    data = execute_query(query, watermark=last_updated)
    
    # Transform
    transformed = transform_data(data)
    
    # Load with MERGE (upsert)
    upsert_data("target_db", "orders", transformed, key_columns=['order_id'])
    
    # Update watermark
    new_watermark = max(row['updated_at'] for row in data)
    update_watermark("orders_watermark", new_watermark)
```

### ID-Based Incremental
```python
def id_based_incremental():
    last_id = get_watermark("orders_id_watermark")
    
    query = f"""
        SELECT * FROM orders 
        WHERE order_id > :last_id 
        ORDER BY order_id
    """
    data = execute_query(query, last_id=last_id)
    
    # Process and load
    transformed = transform_data(data)
    bulk_insert("target_db", "orders", transformed)
    
    # Update watermark
    new_id = max(row['order_id'] for row in data)
    update_watermark("orders_id_watermark", new_id)
```

## Transformation Patterns

### Data Cleansing
```python
def cleanse_data(row):
    return {
        'customer_id': int(row['customer_id']),
        'amount': Decimal(row['amount']).quantize(Decimal('0.01')),
        'email': row['email'].strip().lower(),
        'phone': re.sub(r'[^0-9]', '', row['phone']),
        'country': row['country'].upper()
    }
```

### Data Enrichment
```python
def enrich_order(order):
    # Look up customer data
    customer = lookup_customer(order['customer_id'])
    
    # Add derived fields
    order['customer_segment'] = customer['segment']
    order['customer_lifetime_value'] = customer['ltv']
    order['discount_amount'] = order['total'] * order['discount_rate']
    
    return order
```

### Aggregation
```python
def aggregate_daily_metrics(orders):
    """Aggregate order metrics by day"""
    from collections import defaultdict
    
    daily_metrics = defaultdict(lambda: {
        'order_count': 0,
        'total_amount': Decimal('0'),
        'unique_customers': set()
    })
    
    for order in orders:
        date = order['order_date'].date()
        metrics = daily_metrics[date]
        
        metrics['order_count'] += 1
        metrics['total_amount'] += order['amount']
        metrics['unique_customers'].add(order['customer_id'])
    
    # Convert to list of dicts
    return [
        {
            'date': date,
            'order_count': m['order_count'],
            'total_amount': m['total_amount'],
            'unique_customers': len(m['unique_customers'])
        }
        for date, m in daily_metrics.items()
    ]
```

## UPSERT (MERGE) Pattern

### SQL MERGE
```sql
MERGE INTO target_orders t
USING staging_orders s
ON (t.order_id = s.order_id)
WHEN MATCHED THEN
    UPDATE SET
        t.amount = s.amount,
        t.status = s.status,
        t.updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (order_id, amount, status, created_at, updated_at)
    VALUES (s.order_id, s.amount, s.status, s.created_at, s.updated_at);
```

### Python UPSERT Implementation
```python
def upsert_batch(conn, table, data, key_columns):
    """Generic upsert implementation"""
    staging_table = f"{table}_staging"
    
    # Create staging table
    create_staging_table(conn, staging_table, table)
    
    # Load data to staging
    bulk_insert(conn, staging_table, data)
    
    # Build MERGE statement
    merge_sql = build_merge_sql(table, staging_table, key_columns)
    
    # Execute MERGE
    execute_query(conn, merge_sql)
    
    # Drop staging table
    drop_table(conn, staging_table)
```

## Error Handling

### Reject Records Pattern
```python
def process_with_rejects(data):
    valid_records = []
    rejected_records = []
    
    for row in data:
        try:
            validated = validate_row(row)
            valid_records.append(validated)
        except ValidationError as e:
            rejected_records.append({
                'data': row,
                'error': str(e),
                'timestamp': datetime.now()
            })
    
    # Load valid records
    load_data(valid_records)
    
    # Log rejected records
    log_rejections(rejected_records)
    
    return {
        'processed': len(valid_records),
        'rejected': len(rejected_records)
    }
```

## Best Practices

1. **Use incremental loads** when possible
2. **Implement watermarking** for tracking
3. **Design idempotent operations**
4. **Handle data quality** issues gracefully
5. **Log rejected records** for review
6. **Use MERGE/UPSERT** for updates
7. **Batch operations** for performance
8. **Monitor pipeline metrics**
9. **Test with production data volumes**
10. **Document transformation logic**
