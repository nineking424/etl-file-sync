---
name: airflow-architect
description: Design and optimize Apache Airflow DAGs and workflows including task dependency design, operator selection, and parallel execution strategies
---

# Airflow Architect Skill

## Overview

The Airflow Architect skill provides expert guidance for designing, implementing, and optimizing Apache Airflow DAGs and workflows. This skill covers DAG structure design, task dependency management, operator selection, parallel execution strategies, and dynamic DAG generation patterns.

## Core Capabilities

### 1. DAG Structure Design
- Design modular, maintainable DAG structures
- Implement proper task dependency chains
- Configure DAG scheduling and triggers
- Design idempotent and rerunnable workflows
- Implement error handling and retry strategies

### 2. Task Dependency Optimization
- Design efficient dependency graphs
- Minimize critical path length
- Implement parallel task execution
- Use task groups for logical organization
- Configure branch operators for conditional logic

### 3. Operator Selection and Usage
- Choose appropriate operators (Bash, Python, SQL, etc.)
- Implement custom operators for reusability
- Use sensors for external system polling
- Configure operator-specific parameters
- Implement XCom for inter-task communication

### 4. Parallel Execution Strategies
- Configure executor types (Sequential, Local, Celery, Kubernetes)
- Optimize concurrency and parallelism settings
- Design pool-based resource management
- Implement dynamic task mapping
- Configure priority weights for task scheduling

### 5. Dynamic DAG Generation
- Generate DAGs programmatically from configuration
- Implement factory patterns for DAG creation
- Use Jinja templating for dynamic tasks
- Generate tasks from database or API metadata
- Implement multi-tenant DAG patterns

## Workflow

When designing Airflow workflows:

### 1. Requirements Analysis
- Identify data sources and destinations
- Determine scheduling requirements
- Define SLA and retry policies
- Assess dependency relationships
- Evaluate resource requirements

### 2. DAG Design
- Sketch dependency graph
- Design task boundaries and responsibilities
- Plan for failure scenarios
- Design monitoring and alerting
- Document workflow logic

### 3. Implementation
- Create DAG definition files
- Implement task operators
- Configure dependencies and triggers
- Set up error handling and retries
- Implement testing strategies

### 4. Optimization
- Analyze DAG run durations
- Optimize task parallelism
- Tune executor configurations
- Implement resource pools
- Reduce task overhead

### 5. Monitoring
- Set up SLA monitoring
- Configure task failure alerts
- Monitor resource utilization
- Track DAG run statistics
- Review and refine periodically

## Usage Examples

### Example 1: Production-Ready DAG
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['production', 'etl'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders_func,
        pool='database_pool',
    )
    
    extract_customers = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers_func,
        pool='database_pool',
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_func,
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load_func,
        pool='database_pool',
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> [extract_orders, extract_customers] >> transform >> load >> end
```

### Example 2: Dynamic Task Mapping
```python
from airflow.decorators import task

@task
def get_partition_list():
    return ['2024-01', '2024-02', '2024-03', '2024-04']

@task
def process_partition(partition: str):
    print(f"Processing partition: {partition}")
    # Process data for partition

with DAG('dynamic_tasks', ...) as dag:
    partitions = get_partition_list()
    process_partition.expand(partition=partitions)
```

## Integration Points

### With Kafka Expert
- Design Kafka consumer DAGs
- Implement offset management strategies
- Monitor consumer lag in Airflow
- Design exactly-once processing workflows

### With Database Optimizer
- Optimize SQL queries in SQLOperator
- Configure connection pools for database tasks
- Implement efficient batch processing
- Design incremental load patterns

### With K8s Specialist
- Deploy Airflow on Kubernetes
- Configure KubernetesExecutor
- Implement KubernetesPodOperator tasks
- Design resource allocation strategies

## Best Practices

1. **Keep DAGs simple** and focused
2. **Use task groups** for logical organization
3. **Implement idempotent tasks** for safe reruns
4. **Configure appropriate retries** and timeouts
5. **Use pools** for resource management
6. **Monitor SLAs** and task durations
7. **Test DAGs** before deployment
8. **Document workflows** and dependencies
9. **Use XCom sparingly** (can cause performance issues)
10. **Regular cleanup** of old DAG runs and logs
