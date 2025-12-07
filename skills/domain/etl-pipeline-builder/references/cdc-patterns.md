# CDC Patterns

Change Data Capture implementation patterns and best practices.

## Log-Based CDC

### Debezium Configuration
```json
{
  "name": "oracle-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "database.hostname": "oracle.example.com",
    "database.port": "1521",
    "database.user": "cdc_user",
    "database.dbname": "PRODDB",
    "database.server.name": "oracle-prod",
    "table.include.list": "SCHEMA.ORDERS,SCHEMA.CUSTOMERS",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes",
    "snapshot.mode": "initial",
    "log.mining.strategy": "online_catalog",
    "log.mining.batch.size.default": 1000,
    "tombstones.on.delete": "true"
  }
}
```

### Consuming CDC Events
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'oracle-prod.SCHEMA.ORDERS',
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    event = message.value
    
    if event['payload']['op'] == 'c':  # Create
        handle_insert(event['payload']['after'])
    elif event['payload']['op'] == 'u':  # Update
        handle_update(event['payload']['before'], event['payload']['after'])
    elif event['payload']['op'] == 'd':  # Delete
        handle_delete(event['payload']['before'])
```

## Trigger-Based CDC

### Create CDC Triggers
```sql
-- Create CDC audit table
CREATE TABLE orders_cdc (
    operation VARCHAR(10),
    order_id NUMBER,
    old_data CLOB,
    new_data CLOB,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert trigger
CREATE OR REPLACE TRIGGER orders_insert_cdc
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
    INSERT INTO orders_cdc (operation, order_id, new_data)
    VALUES ('INSERT', :NEW.order_id, 
            JSON_OBJECT('order_id' VALUE :NEW.order_id,
                       'customer_id' VALUE :NEW.customer_id,
                       'amount' VALUE :NEW.amount));
END;

-- Update trigger
CREATE OR REPLACE TRIGGER orders_update_cdc
AFTER UPDATE ON orders
FOR EACH ROW
BEGIN
    INSERT INTO orders_cdc (operation, order_id, old_data, new_data)
    VALUES ('UPDATE', :NEW.order_id,
            JSON_OBJECT('amount' VALUE :OLD.amount),
            JSON_OBJECT('amount' VALUE :NEW.amount));
END;

-- Delete trigger
CREATE OR REPLACE TRIGGER orders_delete_cdc
AFTER DELETE ON orders
FOR EACH ROW
BEGIN
    INSERT INTO orders_cdc (operation, order_id, old_data)
    VALUES ('DELETE', :OLD.order_id,
            JSON_OBJECT('order_id' VALUE :OLD.order_id));
END;
```

### Process CDC Table
```python
def process_cdc_table():
    last_processed = get_watermark("orders_cdc")
    
    query = """
        SELECT * FROM orders_cdc
        WHERE changed_at > :watermark
        ORDER BY changed_at
    """
    changes = execute_query(query, watermark=last_processed)
    
    for change in changes:
        if change['operation'] == 'INSERT':
            handle_insert(json.loads(change['new_data']))
        elif change['operation'] == 'UPDATE':
            handle_update(
                json.loads(change['old_data']),
                json.loads(change['new_data'])
            )
        elif change['operation'] == 'DELETE':
            handle_delete(json.loads(change['old_data']))
    
    # Update watermark
    if changes:
        new_watermark = max(c['changed_at'] for c in changes)
        update_watermark("orders_cdc", new_watermark)
```

## Timestamp-Based CDC

### Simple Timestamp Tracking
```python
def timestamp_based_cdc():
    """Detect changes using updated_at timestamp"""
    last_sync = get_watermark("orders_timestamp")
    
    # Get changed records
    query = """
        SELECT * FROM orders
        WHERE updated_at > :last_sync
        OR (created_at > :last_sync AND updated_at IS NULL)
        ORDER BY COALESCE(updated_at, created_at)
    """
    changes = execute_query(query, last_sync=last_sync)
    
    # Process changes
    for record in changes:
        # Determine if insert or update
        exists = check_exists_in_target(record['order_id'])
        if exists:
            update_target(record)
        else:
            insert_target(record)
    
    # Update watermark
    if changes:
        new_sync = max(
            c.get('updated_at') or c['created_at'] 
            for c in changes
        )
        update_watermark("orders_timestamp", new_sync)
```

## Handling Deletes

### Soft Delete Pattern
```sql
-- Source table with soft delete
ALTER TABLE orders ADD deleted_at TIMESTAMP;

-- CDC query includes soft deletes
SELECT *,
    CASE WHEN deleted_at IS NOT NULL THEN 'DELETE' ELSE 'UPSERT' END as operation
FROM orders
WHERE updated_at > :last_sync OR deleted_at > :last_sync;
```

### Tombstone Pattern
```python
def handle_tombstone(event):
    """Process Kafka tombstone (null value) as delete"""
    if event.value is None:
        # Tombstone indicates delete
        order_id = event.key
        delete_from_target(order_id)
    else:
        # Regular event
        upsert_to_target(event.value)
```

## Best Practices

1. **Use log-based CDC** for real-time replication
2. **Handle schema evolution** gracefully
3. **Preserve operation type** (insert/update/delete)
4. **Implement dead letter queue** for failed events
5. **Monitor CDC lag** continuously
6. **Test failover scenarios**
7. **Document CDC strategy** clearly
8. **Use exactly-once** semantics when possible
9. **Handle deletes explicitly**
10. **Version CDC processors** for reproducibility
