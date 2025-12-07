---
name: etl-pipeline-builder
description: Design robust ETL/ELT pipelines for data integration including CDC patterns, idempotent processing, and schema evolution handling
---

# ETL Pipeline Builder Skill

## Overview

The ETL Pipeline Builder skill provides expert guidance for designing and implementing robust data integration pipelines. This skill covers ETL/ELT pattern selection, CDC implementation, idempotent processing design, schema evolution handling, and error recovery strategies.

## Core Capabilities

### 1. ETL/ELT Pipeline Design
- Choose between ETL and ELT based on requirements
- Design data extraction strategies (full, incremental, CDC)
- Implement transformation logic (mapping, cleansing, aggregation)
- Design load patterns (batch, streaming, micro-batch)
- Optimize pipeline performance and resource usage

### 2. CDC Pattern Implementation
- Implement log-based CDC with Debezium/Oracle GoldenGate
- Design trigger-based CDC solutions
- Implement timestamp-based change detection
- Handle deletes and schema changes in CDC
- Optimize CDC latency and throughput

### 3. Idempotency Design
- Design idempotent insert/update/delete operations
- Implement upsert (MERGE) patterns
- Use natural keys vs surrogate keys
- Handle duplicate processing gracefully
- Design reprocessing-safe transformations

### 4. Schema Evolution Handling
- Implement forward and backward compatible schemas
- Handle column additions and deletions
- Manage data type changes
- Version schemas appropriately
- Use schema registries (Avro, Protobuf)

### 5. Error Handling and Recovery
- Implement retry strategies with exponential backoff
- Design dead letter queues for failed records
- Implement circuit breakers for downstream failures
- Create monitoring and alerting for pipeline health
- Design rollback and recovery procedures

## Workflow

When building data pipelines:

### 1. Requirements Gathering
- Identify source and target systems
- Determine data volume and velocity
- Define SLA requirements
- Assess data quality needs
- Evaluate security and compliance requirements

### 2. Design Phase
- Select ETL vs ELT approach
- Choose extraction method (full, incremental, CDC)
- Design transformation logic
- Plan load strategy
- Design error handling and monitoring

### 3. Implementation
- Implement extraction logic
- Build transformation pipelines
- Configure load operations
- Implement idempotency patterns
- Set up monitoring and alerts

### 4. Testing
- Test with production-like data volumes
- Validate data quality and accuracy
- Test error handling scenarios
- Verify idempotency
- Load test for performance

### 5. Optimization
- Tune batch sizes and parallelism
- Optimize query performance
- Reduce transformation overhead
- Implement caching where appropriate
- Monitor and adjust resource allocation

## Usage Examples

### Example 1: Incremental Load with Watermark
```python
# Track last processed timestamp
def get_last_watermark(table_name):
    query = f"SELECT MAX(updated_at) FROM {table_name}_watermark"
    return execute_query(query)

def extract_incremental(source_table, watermark):
    query = f"""
        SELECT * FROM {source_table}
        WHERE updated_at > :watermark
        ORDER BY updated_at
    """
    return execute_query(query, watermark=watermark)

def load_with_upsert(data, target_table):
    # Idempotent MERGE operation
    merge_sql = f"""
        MERGE INTO {target_table} t
        USING staging_table s
        ON (t.id = s.id)
        WHEN MATCHED THEN
            UPDATE SET t.updated_at = s.updated_at, t.data = s.data
        WHEN NOT MATCHED THEN
            INSERT (id, updated_at, data) VALUES (s.id, s.updated_at, s.data)
    """
    execute_query(merge_sql)

# Update watermark
def update_watermark(table_name, new_watermark):
    query = f"UPDATE {table_name}_watermark SET watermark = :watermark"
    execute_query(query, watermark=new_watermark)
```

### Example 2: CDC with Debezium
```json
{
  "name": "orders-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "database.hostname": "oracle-db",
    "database.port": "1521",
    "database.user": "cdc_user",
    "database.dbname": "PRODDB",
    "database.server.name": "oracle-prod",
    "table.include.list": "SCHEMA.ORDERS,SCHEMA.ORDER_ITEMS",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.orders",
    "snapshot.mode": "initial",
    "log.mining.strategy": "online_catalog",
    "log.mining.batch.size.default": 1000,
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false"
  }
}
```

### Example 3: Schema Evolution with Avro
```python
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Version 1 schema
schema_v1 = {
    "type": "record",
    "name": "Order",
    "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"}
    ]
}

# Version 2 schema - backward compatible
schema_v2 = {
    "type": "record",
    "name": "Order",
    "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string", "default": "USD"}  # New field with default
    ]
}

# Producer with schema registry
producer = AvroProducer({
    'bootstrap.servers': 'kafka:9092',
    'schema.registry.url': 'http://schema-registry:8081'
}, default_value_schema=avro.loads(json.dumps(schema_v2)))
```

## Integration Points

### With Kafka Expert
- Design Kafka-based streaming ETL pipelines
- Implement exactly-once processing semantics
- Use Kafka Connect for data integration
- Design topic partitioning for parallel processing

### With Database Optimizer
- Optimize bulk extraction queries
- Tune batch insert/update operations
- Design efficient incremental load queries
- Optimize connection pool usage

### With Airflow Architect
- Orchestrate ETL workflows with Airflow
- Implement data quality checks as Airflow tasks
- Design retry and recovery in DAGs
- Monitor pipeline SLAs

## Best Practices

1. **Design for idempotency** from the start
2. **Use CDC** for real-time data integration
3. **Implement proper error handling** and DLQs
4. **Version schemas** for evolution
5. **Monitor pipeline health** continuously
6. **Test with production data volumes**
7. **Document data lineage** and transformations
8. **Implement data quality checks**
9. **Use incremental loads** when possible
10. **Design for exactly-once** processing semantics
