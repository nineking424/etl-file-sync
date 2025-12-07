# Error Handling

Robust error handling patterns for ETL/ELT pipelines.

## Retry Strategies

### Exponential Backoff
```python
import time
from functools import wraps

def retry_with_backoff(max_retries=3, base_delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except RetryableException as e:
                    if attempt == max_retries - 1:
                        raise
                    delay = base_delay * (2 ** attempt)
                    print(f"Retry {attempt + 1}/{max_retries} after {delay}s: {e}")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry_with_backoff(max_retries=5, base_delay=2)
def fetch_data_from_api():
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()
```

### Circuit Breaker
```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.timeout:
                self.state = 'HALF_OPEN'
            else:
                raise CircuitBreakerOpen("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
            raise
```

## Dead Letter Queue

### DLQ Implementation
```python
def process_with_dlq(records):
    """Process records with dead letter queue for failures"""
    processed = []
    failed = []
    
    for record in records:
        try:
            result = process_record(record)
            processed.append(result)
        except Exception as e:
            failed.append({
                'record': record,
                'error': str(e),
                'error_type': type(e).__name__,
                'timestamp': datetime.now(),
                'stack_trace': traceback.format_exc()
            })
    
    # Store failed records for later analysis
    if failed:
        store_in_dlq(failed)
    
    return {
        'processed': len(processed),
        'failed': len(failed)
    }
```

## Data Quality Checks

### Validation Framework
```python
class DataValidator:
    def __init__(self):
        self.errors = []
    
    def validate_not_null(self, value, field_name):
        if value is None:
            self.errors.append(f"{field_name} is null")
        return self
    
    def validate_range(self, value, field_name, min_val, max_val):
        if not (min_val <= value <= max_val):
            self.errors.append(
                f"{field_name} out of range: {value} not in [{min_val}, {max_val}]"
            )
        return self
    
    def validate_format(self, value, field_name, pattern):
        if not re.match(pattern, str(value)):
            self.errors.append(f"{field_name} invalid format: {value}")
        return self
    
    def is_valid(self):
        return len(self.errors) == 0

def validate_order(order):
    validator = DataValidator()
    
    validator.validate_not_null(order.get('order_id'), 'order_id')
    validator.validate_not_null(order.get('customer_id'), 'customer_id')
    validator.validate_range(order.get('amount', 0), 'amount', 0, 1000000)
    validator.validate_format(order.get('email', ''), 'email', r'^[\w\.-]+@[\w\.-]+\.\w+$')
    
    if not validator.is_valid():
        raise ValidationError(f"Validation failed: {validator.errors}")
    
    return order
```

## Partial Failure Handling

### Batch with Partial Success
```python
def batch_process_with_partial_success(records, batch_size=1000):
    """Process in batches, continue on partial failure"""
    results = {
        'total': len(records),
        'processed': 0,
        'failed': 0,
        'failed_batches': []
    }
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        try:
            process_batch(batch)
            results['processed'] += len(batch)
        except Exception as e:
            # Log failed batch but continue
            results['failed'] += len(batch)
            results['failed_batches'].append({
                'start_index': i,
                'end_index': i + len(batch),
                'error': str(e)
            })
            
            # Optionally: Try processing records individually
            for record in batch:
                try:
                    process_record(record)
                    results['processed'] += 1
                    results['failed'] -= 1
                except:
                    store_in_dlq([record])
    
    return results
```

## Best Practices

1. **Implement retries** with exponential backoff
2. **Use circuit breakers** for downstream failures
3. **Maintain dead letter queues** for failed records
4. **Validate data quality** before processing
5. **Handle partial failures** gracefully
6. **Log errors** with context
7. **Alert on high error rates**
8. **Implement rollback** procedures
9. **Monitor error metrics**
10. **Regular DLQ review** and reprocessing
