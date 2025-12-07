# Troubleshooting

Common Airflow issues and resolution strategies.

## Scheduler Issues

### DAG Not Showing Up
```bash
# Check for parsing errors
airflow dags list-import-errors

# Test DAG parsing
python path/to/dag_file.py

# Check scheduler logs
airflow scheduler --debug
```

### Tasks Not Scheduling
```python
# Check these settings
catchup = False  # Don't backfill automatically
is_paused_upon_creation = False  # Start active
start_date = days_ago(1)  # Not in the future
```

## Task Execution Issues

### Task Stuck in Queued
```bash
# Check executor capacity
# Check celery workers if using CeleryExecutor
celery -A airflow.executors.celery_executor inspect active

# Check pool capacity
airflow pools list
```

### Task Timeout
```python
# Set appropriate timeout
task = PythonOperator(
    task_id='long_task',
    execution_timeout=timedelta(hours=6)
)
```

## Database Issues

### Metastore Connection Pool Exhausted
```python
# airflow.cfg
[database]
sql_alchemy_pool_size = 10
sql_alchemy_pool_recycle = 3600
sql_alchemy_max_overflow = 20
```

### Zombie Tasks
```bash
# Scheduler detects and kills zombie tasks
# Check for hanging processes
ps aux | grep airflow
```

## Performance Issues

### Slow DAG Parsing
```python
# Minimize top-level code
# Use lazy imports
# Avoid dynamic DAG generation in DAG file
```

### Slow Task Execution
```bash
# Check task logs
airflow tasks test dag_id task_id execution_date

# Monitor resources
htop
```

## Common Solutions

### Clear Task State
```bash
# Clear failed task
airflow tasks clear dag_id --task-regex task_id

# Clear all tasks for date range
airflow tasks clear dag_id --start-date 2024-01-01 --end-date 2024-01-31
```

### Restart Services
```bash
# Restart scheduler
airflow scheduler &

# Restart webserver
airflow webserver -p 8080 &

# Restart workers (Celery)
airflow celery worker &
```

## Best Practices

1. **Monitor scheduler logs** regularly
2. **Set appropriate timeouts**
3. **Check pool capacity** for queued tasks
4. **Use reschedule mode** for sensors
5. **Clear old task instances** periodically
6. **Monitor database connections**
7. **Check executor capacity**
8. **Use debug mode** for troubleshooting
9. **Review task logs** for errors
10. **Keep Airflow updated**
