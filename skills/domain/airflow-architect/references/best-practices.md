# Best Practices

Airflow best practices for production deployments.

## DAG Design

### Keep DAGs Idempotent
```python
# Bad: Not idempotent
def process_data(**context):
    data = load_from_source()
    append_to_table(data)  # Duplicates on rerun

# Good: Idempotent
def process_data(**context):
    execution_date = context['execution_date']
    data = load_from_source(execution_date)
    delete_partition(execution_date)  # Delete first
    insert_into_table(data)  # Then insert
```

### Proper Task Configuration
```python
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,  # Don't block on past failures
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}
```

### Resource Management
```python
# Use pools for resource control
task = PythonOperator(
    task_id='db_task',
    pool='database_pool',  # Limit concurrent DB connections
    pool_slots=2  # This task uses 2 slots
)
```

## Performance Optimization

### Parallelism Configuration
```python
# airflow.cfg
[core]
parallelism = 32  # Max tasks across all DAGs
max_active_tasks_per_dag = 16  # Max tasks per DAG
max_active_runs_per_dag = 1  # Concurrent DAG runs

[scheduler]
max_tis_per_query = 512
```

### Efficient DAG Loading
```python
# Bad: Dynamic import in DAG file
import expensive_module

# Good: Lazy import
def task_function():
    import expensive_module
    expensive_module.process()
```

## Monitoring and Alerting

### SLA Configuration
```python
with DAG(
    'important_dag',
    sla_miss_callback=notify_sla_miss,
    default_args={'sla': timedelta(hours=2)}
) as dag:
    task = PythonOperator(...)
```

### Task Callbacks
```python
def on_failure_callback(context):
    print(f"Task {context['task_instance']} failed")
    # Send alert to monitoring system

task = PythonOperator(
    task_id='critical_task',
    on_failure_callback=on_failure_callback
)
```

## Security

### Use Connections
```python
# Store credentials in Airflow connections, not code
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('my_database')
password = conn.password  # Securely retrieved
```

### Variables for Configuration
```python
from airflow.models import Variable

# Store configuration in Variables
api_key = Variable.get('api_key', default_var='')
```

## Testing

### Unit Testing DAGs
```python
import pytest
from airflow.models import DagBag

def test_dag_loads():
    dagbag = DagBag()
    assert len(dagbag.import_errors) == 0

def test_dag_structure():
    dagbag = DagBag()
    dag = dagbag.get_dag('my_dag')
    assert len(dag.tasks) == 5
    assert dag.has_task('extract')
```

## Best Practices Summary

1. **Make tasks idempotent**
2. **Use pools for resource management**
3. **Configure appropriate retries**
4. **Set task timeouts**
5. **Monitor SLAs**
6. **Use connections for credentials**
7. **Implement proper logging**
8. **Test DAGs before deployment**
9. **Use task groups** for organization
10. **Regular cleanup** of old DAG runs
