---
name: debugger
description: Analyze errors and find root causes using systematic debugging approaches including stack trace analysis, 5 Whys methodology, and log parsing
---

# Debugger Skill

## Overview

The Debugger skill helps you systematically analyze errors, diagnose issues, and identify root causes. It applies structured debugging methodologies including stack trace interpretation, log analysis, 5 Whys root cause analysis, and reproduction scenario development.

## Core Capabilities

### 1. Stack Trace Analysis
- Parse and interpret stack traces across languages
- Identify the exact line and file causing the error
- Trace error propagation through call stack
- Distinguish between symptoms and root causes
- Identify third-party library issues

### 2. Error Message Interpretation
- Decode cryptic error messages
- Identify error categories (syntax, runtime, logical)
- Extract relevant context from error messages
- Map errors to likely causes
- Suggest investigation paths

### 3. 5 Whys Root Cause Analysis
- Apply systematic questioning to find root causes
- Distinguish symptoms from underlying problems
- Identify contributing factors
- Prevent recurring issues
- Document findings

### 4. Log Parsing and Analysis
- Extract relevant information from logs
- Correlate events across log entries
- Identify patterns and anomalies
- Track request flows through systems
- Detect timing and sequence issues

### 5. Reproduction Scenario Development
- Create minimal reproducible examples
- Identify necessary conditions for bug occurrence
- Document step-by-step reproduction steps
- Isolate variables affecting the bug
- Provide debugging context

## Workflow

### 1. Error Collection
- Gather complete error messages
- Capture full stack traces
- Collect relevant logs
- Note system state and context
- Document user actions leading to error

### 2. Error Classification
- **Syntax Error**: Code doesn't compile/parse
- **Runtime Error**: Crash during execution
- **Logical Error**: Wrong output, no crash
- **Performance Issue**: Slow or resource-intensive
- **Integration Error**: External system failure

### 3. Stack Trace Analysis
- Start from the top (most recent call)
- Identify your code vs. framework code
- Find the first occurrence in your code
- Examine variable states and inputs
- Check for null/undefined values

### 4. Hypothesis Formation
- Develop theories about root cause
- Identify testable assumptions
- Prioritize likely causes
- Plan verification approach

### 5. Root Cause Investigation (5 Whys)
- Ask "Why did this happen?" repeatedly
- Dig deeper with each answer
- Continue until reaching fundamental cause
- Document the chain of reasoning

### 6. Reproduction
- Create minimal test case
- Document exact steps
- Identify required conditions
- Verify consistency
- Share reproduction scenario

## Debugging Checklist

### Initial Assessment
- [ ] Collect complete error message
- [ ] Capture full stack trace
- [ ] Note when error first occurred
- [ ] Identify affected users/scenarios
- [ ] Check if error is consistent or intermittent
- [ ] Document recent changes to codebase

### Stack Trace Analysis
- [ ] Identify error type and message
- [ ] Find first occurrence in your code
- [ ] Check method signatures and parameters
- [ ] Examine variable values at failure point
- [ ] Trace data flow backward
- [ ] Check for null/undefined values

### Log Analysis
- [ ] Search logs around error timestamp
- [ ] Correlate request ID across logs
- [ ] Check for warnings before error
- [ ] Examine database query logs
- [ ] Review API request/response logs
- [ ] Look for resource exhaustion indicators

### Environment Check
- [ ] Verify environment configuration
- [ ] Check dependency versions
- [ ] Review environment variables
- [ ] Validate database connections
- [ ] Check file permissions
- [ ] Verify network connectivity

### Root Cause Analysis
- [ ] Apply 5 Whys methodology
- [ ] Distinguish symptom from cause
- [ ] Identify contributing factors
- [ ] Check for similar past issues
- [ ] Document findings
- [ ] Propose prevention measures

### Reproduction
- [ ] Create minimal reproducible example
- [ ] Document exact steps
- [ ] List required conditions
- [ ] Test in clean environment
- [ ] Verify consistency
- [ ] Share with team

## Usage Examples

### Example 1: Python Stack Trace Analysis

**Error:**
```
Traceback (most recent call last):
  File "app.py", line 45, in process_order
    total = calculate_total(order)
  File "app.py", line 78, in calculate_total
    discount = get_discount(order.customer_id)
  File "app.py", line 102, in get_discount
    return customer.discount_rate * 100
AttributeError: 'NoneType' object has no attribute 'discount_rate'
```

**Analysis:**
1. **Error Type**: AttributeError - trying to access attribute on None
2. **Location**: Line 102 in `get_discount` function
3. **Root Cause**: `customer` is None, meaning `get_customer()` returned None
4. **Propagation**: Error started in `get_discount`, called by `calculate_total`, called by `process_order`

**Investigation Path:**
```python
# Line 102: customer is None - why?
def get_discount(customer_id):
    customer = db.query(Customer).get(customer_id)  # This returned None
    return customer.discount_rate * 100

# Hypothesis: customer_id doesn't exist in database
# or customer_id is None
```

**Fix:**
```python
def get_discount(customer_id):
    if not customer_id:
        return 0

    customer = db.query(Customer).get(customer_id)
    if not customer:
        logging.warning(f"Customer {customer_id} not found")
        return 0

    return customer.discount_rate * 100
```

---

### Example 2: JavaScript Asynchronous Error

**Error:**
```javascript
UnhandledPromiseRejectionWarning: TypeError: Cannot read property 'name' of undefined
    at processUser (/app/user.js:23:35)
    at async getUserData (/app/user.js:15:18)
    at async /app/api.js:42:20
```

**Analysis:**
1. **Error Type**: TypeError - accessing property on undefined
2. **Async Context**: Error in async function chain
3. **Location**: Line 23 in user.js

**Investigation:**
```javascript
// Line 23: What's undefined?
async function processUser(userId) {
    const user = await fetchUser(userId);
    console.log(user.name);  // Line 23 - user is undefined
}

// Check fetchUser
async function fetchUser(userId) {
    const response = await fetch(`/api/users/${userId}`);
    const data = await response.json();
    return data.user;  // Returns undefined if data.user doesn't exist
}
```

**Root Cause**: API returns `{ error: "Not found" }` instead of `{ user: {...} }`

**Fix:**
```javascript
async function fetchUser(userId) {
    const response = await fetch(`/api/users/${userId}`);

    if (!response.ok) {
        throw new Error(`User ${userId} not found`);
    }

    const data = await response.json();

    if (!data.user) {
        throw new Error(`Invalid response format`);
    }

    return data.user;
}

async function processUser(userId) {
    try {
        const user = await fetchUser(userId);
        console.log(user.name);
    } catch (error) {
        console.error(`Error processing user ${userId}:`, error);
        throw error;
    }
}
```

---

### Example 3: 5 Whys Analysis

**Problem**: Application crashes in production every night at 2 AM

**5 Whys Investigation:**

**1. Why does the application crash at 2 AM?**
Answer: OutOfMemoryError is thrown

**2. Why is there an OutOfMemoryError?**
Answer: Memory usage grows continuously until heap is full

**3. Why does memory usage grow continuously?**
Answer: Objects are not being garbage collected

**4. Why are objects not being garbage collected?**
Answer: A cache is holding references to user session objects

**5. Why is the cache holding onto session objects indefinitely?**
Answer: Cache has no expiration policy; sessions are never removed

**Root Cause**: Missing cache expiration policy

**Solution**:
```java
// Before: Unbounded cache
private Map<String, Session> sessionCache = new HashMap<>();

public void cacheSession(String id, Session session) {
    sessionCache.put(id, session);  // Never removed!
}

// After: Cache with expiration
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

private Cache<String, Session> sessionCache = Caffeine.newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(30, TimeUnit.MINUTES)
    .build();

public void cacheSession(String id, Session session) {
    sessionCache.put(id, session);
}
```

**Prevention**: Add monitoring for cache size and memory usage

---

### Example 4: Log Analysis

**Problem**: API endpoint returns 500 error intermittently

**Logs:**
```
2025-12-08 10:15:23 INFO  [req-123] GET /api/orders/456
2025-12-08 10:15:23 DEBUG [req-123] Fetching order 456
2025-12-08 10:15:23 DEBUG [req-123] Executing query: SELECT * FROM orders WHERE id = 456
2025-12-08 10:15:28 ERROR [req-123] Database query timeout after 5000ms
2025-12-08 10:15:28 ERROR [req-123] SQLException: Connection pool exhausted
2025-12-08 10:15:28 ERROR [req-123] 500 Internal Server Error
```

**Analysis:**
1. **Request ID**: req-123 allows correlation
2. **Timeline**: 5-second delay before timeout
3. **Error**: Connection pool exhausted
4. **Pattern**: Intermittent suggests resource contention

**Investigation Path:**
```
1. Check connection pool configuration
2. Look for unclosed connections
3. Check concurrent request volume
4. Analyze slow queries
```

**Root Cause Discovery:**
```python
# Found: Connections not returned to pool
def get_orders(order_id):
    conn = pool.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
    result = cursor.fetchone()
    # Missing: pool.return_connection(conn)
    return result

# Connection leak!
```

**Fix:**
```python
def get_orders(order_id):
    conn = pool.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
        return cursor.fetchone()
    finally:
        pool.return_connection(conn)  # Always return
```

---

### Example 5: Race Condition Debugging

**Problem**: Counter shows wrong value in multi-threaded application

**Reproduction Scenario:**
```python
# Minimal reproducible example
import threading

counter = 0

def increment():
    global counter
    for _ in range(100000):
        counter += 1  # Not atomic!

# Expected: 200,000
# Actual: ~150,000 (varies)
threads = [threading.Thread(target=increment) for _ in range(2)]
for t in threads:
    t.start()
for t in threads:
    t.join()

print(f"Counter: {counter}")  # Wrong value
```

**Analysis:**
```
counter += 1 is actually:
1. temp = counter      (Read)
2. temp = temp + 1     (Modify)
3. counter = temp      (Write)

Thread interleaving causes lost updates:
Thread 1: Read (100)
Thread 2: Read (100)
Thread 1: Write (101)
Thread 2: Write (101)  <- Lost Thread 1's update!
```

**Fix:**
```python
import threading

counter = 0
lock = threading.Lock()

def increment():
    global counter
    for _ in range(100000):
        with lock:
            counter += 1  # Atomic with lock

# Or use atomic type
from threading import atomic_int
counter = atomic_int(0)

def increment():
    for _ in range(100000):
        counter.fetch_add(1)  # Atomic operation
```

## Integration Points

### With Security Auditor
- Debug security vulnerabilities
- Analyze exploit attempts in logs
- Trace attack vectors

### With Performance Analyzer
- Debug performance bottlenecks
- Analyze slow query logs
- Investigate memory issues

### With Tester
- Create test cases from bugs
- Develop regression tests
- Validate fixes

## Debugging Tools

### Python
- **pdb**: Built-in debugger
- **ipdb**: Enhanced interactive debugger
- **pdb++**: Advanced features
- **logging**: Structured logging
- **traceback**: Stack trace utilities

### JavaScript/Node.js
- **Chrome DevTools**: Browser debugging
- **VS Code Debugger**: IDE integration
- **node --inspect**: Remote debugging
- **console methods**: Logging utilities
- **Error.captureStackTrace**: Stack traces

### Java
- **IntelliJ IDEA Debugger**: IDE debugging
- **jdb**: Command-line debugger
- **Log4j/SLF4J**: Logging frameworks
- **Thread dumps**: Analyze deadlocks
- **Heap dumps**: Memory analysis

## Best Practices

1. **Reproduce Consistently**: Always create reliable reproduction steps
2. **Isolate Variables**: Change one thing at a time
3. **Check Assumptions**: Verify what you think is true
4. **Use Logging**: Add strategic log statements
5. **Binary Search**: Divide problem space in half
6. **Read Error Messages**: Don't skip error details
7. **Understand Stack Traces**: Learn to parse them quickly
8. **Document Findings**: Record what you learn
9. **Ask for Help**: Share context when stuck
10. **Take Breaks**: Fresh perspective helps

## Common Debugging Patterns

### Rubber Duck Debugging
Explain the problem out loud to find the solution

### Binary Search Debugging
Comment out half the code to isolate the issue

### Print Debugging
Add logging statements to trace execution

### Breakpoint Debugging
Pause execution and inspect state

### Git Bisect
Find which commit introduced the bug

## References

See the `references/` directory for detailed guides on:
- **debugging-strategies.md**: Systematic debugging approaches and methodologies
- **common-errors.md**: Common error patterns and solutions by language
- **troubleshooting-guide.md**: Step-by-step troubleshooting for specific scenarios
