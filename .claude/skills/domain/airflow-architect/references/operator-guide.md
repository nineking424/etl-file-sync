# Operator Guide

Guide to selecting and using appropriate Airflow operators.

## Core Operators

### PythonOperator
```python
from airflow.operators.python import PythonOperator

def my_function(param1, **context):
    print(f"Parameter: {param1}")
    print(f"Execution date: {context['execution_date']}")

task = PythonOperator(
    task_id='python_task',
    python_callable=my_function,
    op_kwargs={'param1': 'value'},
    provide_context=True
)
```

### BashOperator
```python
from airflow.operators.bash import BashOperator

task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello {{ ds }}"',
    env={'ENV_VAR': 'value'}
)
```

### SQLExecuteQueryOperator
```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

task = SQLExecuteQueryOperator(
    task_id='sql_task',
    conn_id='postgres_default',
    sql='SELECT COUNT(*) FROM users WHERE created_date = {{ ds }}',
    autocommit=True
)
```

## Provider Operators

### KubernetesPodOperator
```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

task = KubernetesPodOperator(
    task_id='k8s_task',
    name='data-processor',
    namespace='airflow',
    image='myapp:latest',
    cmds=['python', 'process.py'],
    arguments=['--date', '{{ ds }}'],
    get_logs=True,
    is_delete_operator_pod=True
)
```

### S3Operator
```python
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

task = S3CreateObjectOperator(
    task_id='s3_upload',
    s3_bucket='my-bucket',
    s3_key='data/{{ ds }}/output.csv',
    data='Hello World',
    aws_conn_id='aws_default'
)
```

## Custom Operators

### Creating Custom Operator
```python
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class MyCustomOperator(BaseOperator):
    @apply_defaults
    def __init__(self, my_param, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.my_param = my_param
    
    def execute(self, context):
        self.log.info(f"Executing with param: {self.my_param}")
        # Custom logic here
        return "result"

task = MyCustomOperator(
    task_id='custom_task',
    my_param='value'
)
```

## Sensor Operators

### FileSensor
```python
from airflow.sensors.filesystem import FileSensor

sensor = FileSensor(
    task_id='wait_for_file',
    filepath='/data/input.csv',
    poke_interval=60,
    timeout=3600,
    mode='reschedule'  # Don't block worker
)
```

### DateTimeSensor
```python
from airflow.sensors.date_time import DateTimeSensor

sensor = DateTimeSensor(
    task_id='wait_until_time',
    target_time='{{ execution_date + macros.timedelta(hours=2) }}',
    mode='reschedule'
)
```

## Best Practices

1. **Use @task decorator** for simple Python functions
2. **Prefer KubernetesPodOperator** for isolated execution
3. **Use sensors with reschedule mode** to avoid blocking
4. **Create custom operators** for reusable logic
5. **Set appropriate timeouts**
6. **Use provider packages** for integrations
7. **Implement proper error handling**
8. **Log meaningful messages**
9. **Return values for XCom** when needed
10. **Test operators independently**
