# Troubleshooting Guide

Step-by-step troubleshooting for specific scenarios and common problems.

## Database Connection Issues

### Symptom: "Connection refused" or "Cannot connect to database"

**Diagnostic Steps:**
```bash
# 1. Check if database is running
# PostgreSQL
sudo systemctl status postgresql
# MySQL
sudo systemctl status mysql
# MongoDB
sudo systemctl status mongod

# 2. Check if port is listening
netstat -tlnp | grep 5432  # PostgreSQL
netstat -tlnp | grep 3306  # MySQL
netstat -tlnp | grep 27017 # MongoDB

# 3. Test connection
# PostgreSQL
psql -h localhost -U username -d database
# MySQL
mysql -h localhost -u username -p database
# MongoDB
mongo mongodb://localhost:27017/database

# 4. Check firewall
sudo ufw status
sudo iptables -L

# 5. Verify credentials
echo $DATABASE_URL
cat config/database.yml
```

**Common Fixes:**
```python
# Fix 1: Check connection string
# Wrong
DATABASE_URL = "postgresql://user:pass@localhost/db"

# Right (include port)
DATABASE_URL = "postgresql://user:pass@localhost:5432/db"

# Fix 2: Increase connection pool
from sqlalchemy import create_engine
engine = create_engine(
    DATABASE_URL,
    pool_size=20,      # Increase from default 5
    max_overflow=40,   # Allow overflow
    pool_pre_ping=True # Verify connections
)

# Fix 3: Handle connection errors
from sqlalchemy.exc import OperationalError
try:
    db.session.execute("SELECT 1")
except OperationalError as e:
    logging.error(f"Database connection failed: {e}")
    # Retry logic
```

---

## API Request Failures

### Symptom: "500 Internal Server Error"

**Debugging Checklist:**
```bash
# 1. Check application logs
tail -f /var/log/app/error.log

# 2. Check web server logs (nginx/apache)
tail -f /var/log/nginx/error.log

# 3. Test with curl
curl -v http://localhost:8000/api/endpoint
# Look for response headers and body

# 4. Check request payload
curl -v -X POST http://localhost:8000/api/endpoint \
  -H "Content-Type: application/json" \
  -d '{"key": "value"}'

# 5. Check authentication
curl -v http://localhost:8000/api/endpoint \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Investigation:**
```python
# Add detailed error logging
import traceback
import logging

@app.errorhandler(500)
def handle_500(error):
    # Log full stack trace
    logging.error(f"500 error: {error}")
    logging.error(traceback.format_exc())

    # Return generic message to client
    return {"error": "Internal server error"}, 500

# Add request logging
@app.before_request
def log_request():
    logging.info(f"{request.method} {request.path}")
    logging.debug(f"Headers: {dict(request.headers)}")
    logging.debug(f"Body: {request.get_data()}")

# Add response logging
@app.after_request
def log_response(response):
    logging.info(f"Response: {response.status_code}")
    return response
```

---

## Performance Degradation

### Symptom: "Application suddenly slow"

**Investigation Steps:**
```bash
# 1. Check CPU usage
top
htop

# 2. Check memory usage
free -m
vmstat 1

# 3. Check disk I/O
iostat -x 1

# 4. Check network
netstat -s
iftop

# 5. Check application metrics
# Response time distribution
# Error rate
# Request rate
# Resource utilization
```

**Common Causes & Solutions:**

**1. Database Query Performance:**
```sql
-- Find slow queries (PostgreSQL)
SELECT pid, now() - pg_stat_activity.query_start AS duration, query
FROM pg_stat_activity
WHERE state = 'active'
ORDER BY duration DESC;

-- Find missing indexes
SELECT schemaname, tablename, attname
FROM pg_stats
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
  AND n_distinct < 0  -- High cardinality
  AND tablename NOT IN (
    SELECT tablename FROM pg_indexes
    WHERE indexdef LIKE '%' || attname || '%'
  );

-- Add index
CREATE INDEX idx_users_email ON users(email);

-- Analyze query plan
EXPLAIN ANALYZE SELECT * FROM users WHERE email = 'user@example.com';
```

**2. Memory Leak:**
```python
# Monitor memory growth
import psutil
import os

def log_memory():
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / 1024 / 1024  # MB
    logging.info(f"Memory usage: {mem:.2f} MB")

# Call periodically
import schedule
schedule.every(1).minutes.do(log_memory)

# Find memory leaks
import tracemalloc
tracemalloc.start()

# ... run code ...

snapshot = tracemalloc.take_snapshot()
top_stats = snapshot.statistics('lineno')

for stat in top_stats[:10]:
    print(stat)
```

**3. Connection Pool Exhaustion:**
```python
# Monitor connection pool
from sqlalchemy import event
from sqlalchemy.pool import Pool

@event.listens_for(Pool, "connect")
def receive_connect(dbapi_conn, connection_record):
    logging.debug("Connection created")

@event.listens_for(Pool, "checkout")
def receive_checkout(dbapi_conn, connection_record, connection_proxy):
    logging.debug(f"Connection checked out. Pool size: {pool.size()}")

# Increase pool size
engine = create_engine(
    DATABASE_URL,
    pool_size=50,
    max_overflow=100
)
```

---

## Authentication/Authorization Failures

### Symptom: "401 Unauthorized" or "403 Forbidden"

**Debugging:**
```python
# 1. Log authentication attempts
@app.before_request
def log_auth():
    auth_header = request.headers.get('Authorization')
    logging.info(f"Auth header: {auth_header}")

# 2. Verify token
import jwt

def verify_token(token):
    try:
        payload = jwt.decode(
            token,
            secret_key,
            algorithms=['HS256']
        )
        logging.info(f"Token payload: {payload}")
        return payload
    except jwt.ExpiredSignatureError:
        logging.warning("Token expired")
        raise
    except jwt.InvalidTokenError as e:
        logging.error(f"Invalid token: {e}")
        raise

# 3. Check permissions
def check_permission(user, resource, action):
    has_permission = user.has_permission(resource, action)
    logging.info(f"User {user.id} permission check: {resource}.{action} = {has_permission}")
    return has_permission
```

---

## Intermittent Failures

### Symptom: "Works sometimes, fails randomly"

**Likely Causes:**
1. Race conditions
2. Network issues
3. Resource exhaustion
4. Timing dependencies
5. External service failures

**Investigation:**
```python
# 1. Add timing information
import time

def process_request():
    start = time.time()
    try:
        result = do_work()
        duration = time.time() - start
        logging.info(f"Success in {duration:.3f}s")
        return result
    except Exception as e:
        duration = time.time() - start
        logging.error(f"Failed after {duration:.3f}s: {e}")
        raise

# 2. Log environment state
def log_environment():
    logging.info(f"Thread count: {threading.active_count()}")
    logging.info(f"CPU usage: {psutil.cpu_percent()}%")
    logging.info(f"Memory usage: {psutil.virtual_memory().percent}%")
    logging.info(f"Open connections: {len(connection_pool._pool)}")

# 3. Retry with exponential backoff
import backoff

@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=3,
    on_backoff=lambda details: logging.warning(f"Retry {details['tries']}")
)
def flaky_operation():
    # Operation that might fail
    pass
```

**Race Condition Detection:**
```python
# Add assertions to check invariants
def transfer_money(from_account, to_account, amount):
    initial_total = from_account.balance + to_account.balance

    from_account.balance -= amount
    to_account.balance += amount

    final_total = from_account.balance + to_account.balance

    # Invariant: total money should be conserved
    assert initial_total == final_total, f"Money disappeared! {initial_total} != {final_total}"
```

---

## Container/Docker Issues

### Symptom: "Container keeps restarting"

**Investigation:**
```bash
# 1. Check container logs
docker logs container_name
docker logs --tail 100 container_name

# 2. Check container status
docker ps -a
docker inspect container_name

# 3. Check resource limits
docker stats container_name

# 4. Check health check
docker inspect container_name | jq '.[0].State.Health'

# 5. Enter container for debugging
docker exec -it container_name /bin/bash

# 6. Check Docker events
docker events --filter container=container_name
```

**Common Fixes:**
```dockerfile
# Fix 1: Application crashes on startup
# Check entrypoint script
CMD ["python", "app.py"]  # Wrong if app.py doesn't exist

# Add error handling to startup
#!/bin/bash
set -e  # Exit on error

echo "Starting application..."
python app.py || {
    echo "Failed to start application"
    exit 1
}

# Fix 2: OOM (Out of Memory)
# Increase memory limit
docker run --memory="2g" myapp

# Or in docker-compose.yml
services:
  app:
    mem_limit: 2g

# Fix 3: Health check failing
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1
```

---

## Environment-Specific Issues

### Symptom: "Works in dev, fails in production"

**Investigation Checklist:**
```python
# 1. Compare environment variables
def compare_environments():
    dev_vars = load_env_file('.env.dev')
    prod_vars = load_env_file('.env.prod')

    for key in set(dev_vars.keys()) | set(prod_vars.keys()):
        dev_val = dev_vars.get(key, 'MISSING')
        prod_val = prod_vars.get(key, 'MISSING')
        if dev_val != prod_val:
            print(f"{key}: dev={dev_val} prod={prod_val}")

# 2. Check Python/Node/Java versions
import sys
print(f"Python: {sys.version}")

# 3. Check dependency versions
pip list > dev_packages.txt
# Compare with production

# 4. Check database differences
# Schema differences
# Data differences
# Configuration differences

# 5. Add environment indicator to logs
logging.info(f"Environment: {os.getenv('ENVIRONMENT')}")
logging.info(f"Debug mode: {app.debug}")
```

---

## Deadlock Debugging

### Symptom: "Application hangs"

**Investigation:**
```python
# 1. Thread dump (Python)
import sys
import traceback

def dump_threads():
    for thread_id, frame in sys._current_frames().items():
        print(f"Thread {thread_id}:")
        traceback.print_stack(frame)
        print()

# 2. Detect potential deadlocks
import threading

def diagnose_deadlock():
    for thread in threading.enumerate():
        print(f"Thread: {thread.name}")
        print(f"Alive: {thread.is_alive()}")
        print(f"Daemon: {thread.daemon}")

# 3. Add timeout to locks
lock = threading.Lock()

if lock.acquire(timeout=5.0):
    try:
        # Critical section
        pass
    finally:
        lock.release()
else:
    logging.error("Failed to acquire lock - possible deadlock")
```

**Java Thread Dump:**
```bash
# Get thread dump
jstack <pid> > threaddump.txt

# Look for:
# - BLOCKED threads
# - Waiting for locks
# - Circular dependencies
```

---

## Summary Troubleshooting Workflow

```
1. REPRODUCE
   Can you make it happen consistently?

2. ISOLATE
   What's the minimal case that demonstrates the issue?

3. COLLECT DATA
   - Logs
   - Metrics
   - Stack traces
   - Environment info

4. FORM HYPOTHESIS
   What might be causing this?

5. TEST HYPOTHESIS
   - Add logging
   - Add assertions
   - Experiment

6. FIX
   Implement solution based on understanding

7. VERIFY
   - Test fix
   - Add regression test
   - Deploy
   - Monitor

8. DOCUMENT
   - What was the issue?
   - Why did it happen?
   - How was it fixed?
   - How to prevent recurrence?
```

**Remember**: Systematic troubleshooting beats random changes every time!
