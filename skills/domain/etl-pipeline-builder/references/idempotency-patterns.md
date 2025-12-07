# Idempotency Patterns

Design patterns for idempotent data pipeline operations.

## UPSERT Pattern

### SQL MERGE
```sql
-- Idempotent MERGE operation
MERGE INTO target_table t
USING source_table s
ON (t.id = s.id)
WHEN MATCHED THEN
    UPDATE SET
        t.value = s.value,
        t.updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (id, value, created_at, updated_at)
    VALUES (s.id, s.value, s.created_at, s.updated_at);

-- Running multiple times produces same result
```

### Python UPSERT
```python
def upsert_records(records, key_columns):
    """Idempotent upsert implementation"""
    conn = get_connection()
    
    for record in records:
        # Check if exists
        key_values = {k: record[k] for k in key_columns}
        existing = find_record(conn, key_values)
        
        if existing:
            # Update
            update_record(conn, record, key_values)
        else:
            # Insert
            insert_record(conn, record)
    
    conn.commit()
```

## Delete-Insert Pattern

### Truncate and Load
```python
def truncate_and_load(table_name, data):
    """Idempotent full table replacement"""
    conn = get_connection()
    
    try:
        # Delete all existing data
        conn.execute(f"TRUNCATE TABLE {table_name}")
        
        # Insert new data
        bulk_insert(conn, table_name, data)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
```

### Partition Delete-Insert
```python
def partition_delete_insert(table_name, partition_key, partition_value, data):
    """Idempotent partition replacement"""
    conn = get_connection()
    
    try:
        # Delete partition
        conn.execute(
            f"DELETE FROM {table_name} WHERE {partition_key} = :value",
            value=partition_value
        )
        
        # Insert new data
        bulk_insert(conn, table_name, data)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
```

## Natural Key Pattern

### Use Business Keys
```python
def process_order(order):
    """Use natural key (order_number) instead of surrogate key"""
    
    # Natural key ensures idempotency
    existing = find_order_by_number(order['order_number'])
    
    if existing:
        # Update if data changed
        if has_changes(existing, order):
            update_order(order['order_number'], order)
    else:
        # Insert new
        insert_order(order)
```

## State Checking Pattern

### Check Before Action
```python
def idempotent_file_processing(file_path):
    """Check if file already processed"""
    
    # Calculate file hash
    file_hash = calculate_hash(file_path)
    
    # Check if already processed
    if is_file_processed(file_hash):
        print(f"File {file_path} already processed, skipping")
        return
    
    # Process file
    data = extract_from_file(file_path)
    load_to_database(data)
    
    # Mark as processed
    mark_file_processed(file_hash, file_path)
```

## Versioning Pattern

### Event Sourcing
```python
def process_event_idempotently(event):
    """Process event with version checking"""
    
    # Check if event already processed
    if event_exists(event['event_id']):
        print(f"Event {event['event_id']} already processed")
        return
    
    # Store event
    store_event(event)
    
    # Apply event to state
    apply_event_to_state(event)
```

## Timestamp-Based Idempotency

### Last Modified Wins
```python
def upsert_with_timestamp(record):
    """Use timestamp for conflict resolution"""
    
    existing = find_record(record['id'])
    
    if existing:
        # Only update if newer
        if record['updated_at'] > existing['updated_at']:
            update_record(record)
        else:
            print(f"Record {record['id']} is older, skipping")
    else:
        insert_record(record)
```

## Best Practices

1. **Use UPSERT/MERGE** for updates
2. **Implement delete-insert** for full loads
3. **Use natural keys** when possible
4. **Check state** before operations
5. **Version events** for event sourcing
6. **Use timestamps** for conflict resolution
7. **Make operations replayable**
8. **Test reprocessing** scenarios
9. **Document idempotency** guarantees
10. **Monitor duplicate processing**
