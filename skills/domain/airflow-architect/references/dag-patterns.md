# DAG Patterns

Common Apache Airflow DAG design patterns and best practices.

## Basic DAG Patterns

### Linear Pipeline
```python
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG('linear_pipeline', ...) as dag:
    extract = PythonOperator(task_id='extract', ...)
    transform = PythonOperator(task_id='transform', ...)
    load = PythonOperator(task_id='load', ...)
    
    extract >> transform >> load
```

### Parallel Branches
```python
with DAG('parallel_branches', ...) as dag:
    start = EmptyOperator(task_id='start')
    
    task_a = PythonOperator(task_id='task_a', ...)
    task_b = PythonOperator(task_id='task_b', ...)
    task_c = PythonOperator(task_id='task_c', ...)
    
    end = EmptyOperator(task_id='end')
    
    start >> [task_a, task_b, task_c] >> end
```

### Conditional Branching
```python
from airflow.operators.python import BranchPythonOperator

def decide_branch(**context):
    if context['execution_date'].day % 2 == 0:
        return 'even_day_task'
    return 'odd_day_task'

with DAG('conditional', ...) as dag:
    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=decide_branch
    )
    
    even_task = PythonOperator(task_id='even_day_task', ...)
    odd_task = PythonOperator(task_id='odd_day_task', ...)
    
    branch >> [even_task, odd_task]
```

## Dynamic DAG Generation

### Task Factory Pattern
```python
def create_task(task_id, table_name):
    return PythonOperator(
        task_id=task_id,
        python_callable=process_table,
        op_kwargs={'table': table_name}
    )

tables = ['orders', 'customers', 'products']

with DAG('dynamic_tasks', ...) as dag:
    for table in tables:
        create_task(f'process_{table}', table)
```

### Dynamic Task Mapping
```python
from airflow.decorators import task

@task
def get_partitions():
    return ['2024-01', '2024-02', '2024-03']

@task
def process_partition(partition):
    print(f"Processing {partition}")

with DAG('dynamic_mapping', ...) as dag:
    partitions = get_partitions()
    process_partition.expand(partition=partitions)
```

## Task Groups

### Logical Grouping
```python
from airflow.utils.task_group import TaskGroup

with DAG('task_groups', ...) as dag:
    start = EmptyOperator(task_id='start')
    
    with TaskGroup('extract_group') as extract_group:
        extract_orders = PythonOperator(task_id='orders', ...)
        extract_customers = PythonOperator(task_id='customers', ...)
    
    with TaskGroup('transform_group') as transform_group:
        transform_orders = PythonOperator(task_id='orders', ...)
        transform_customers = PythonOperator(task_id='customers', ...)
    
    start >> extract_group >> transform_group
```

## Sensor Patterns

### Wait for File
```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/data/input.csv',
    poke_interval=30,
    timeout=3600
)
```

### Wait for External Trigger
```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id='wait_for_upstream_dag',
    external_dag_id='upstream_dag',
    external_task_id='final_task',
    mode='reschedule'
)
```

## XCom Communication

### Push and Pull Values
```python
def push_value(**context):
    context['ti'].xcom_push(key='result', value={'count': 100})

def pull_value(**context):
    result = context['ti'].xcom_pull(task_ids='push_task', key='result')
    print(f"Received: {result}")

with DAG('xcom_pattern', ...) as dag:
    push_task = PythonOperator(task_id='push_task', python_callable=push_value)
    pull_task = PythonOperator(task_id='pull_task', python_callable=pull_value)
    
    push_task >> pull_task
```

## Best Practices

1. **Keep DAGs simple** and focused
2. **Use task groups** for logical organization
3. **Avoid deep task dependencies**
4. **Use dynamic task mapping** for scalability
5. **Implement proper error handling**
6. **Use sensors wisely** (avoid blocking)
7. **Limit XCom usage** (performance impact)
8. **Document DAG logic** clearly
9. **Test DAGs** before deployment
10. **Monitor DAG run duration** and optimize
