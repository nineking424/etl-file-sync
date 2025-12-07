# Debugging Strategies and Methodologies

Systematic approaches to finding and fixing bugs efficiently.

## Fundamental Debugging Strategies

### 1. Understand Before Fixing

**Never change code randomly hoping it will work.**

**Process:**
1. Reproduce the bug consistently
2. Understand what the code is supposed to do
3. Understand what it's actually doing
4. Identify the gap between expected and actual
5. Form hypothesis about why
6. Test hypothesis
7. Fix based on understanding

**Example:**
```python
# Bug: Function returns wrong value
def calculate_discount(price, rate):
    return price * rate  # Returns discount amount

# Expected: price after discount
# Actual: discount amount only
# Hypothesis: Missing subtraction

# Fix based on understanding
def calculate_discount(price, rate):
    discount = price * rate
    return price - discount  # Return final price
```

---

### 2. Reproduce Consistently

**If you can't reproduce it, you can't debug it.**

**Checklist:**
- [ ] Document exact steps to reproduce
- [ ] Note required data/state
- [ ] Identify environment conditions
- [ ] Record timing/sequence
- [ ] Test reproduction reliability (does it happen every time?)

**Example Reproduction Steps:**
```markdown
## Steps to Reproduce
1. Login as user "test@example.com"
2. Navigate to /orders
3. Click "Create Order" button
4. Select product ID 123
5. Submit form
6. **Expected**: Order created successfully
7. **Actual**: Error 500 - NullPointerException

## Environment
- Browser: Chrome 119
- User role: Standard user (not admin)
- Database: Contains product 123 with null supplier_id
- Time: Happens every time

## Key Insight
- Only happens for products with null supplier_id
- Admin users don't see error (different code path)
```

---

### 3. Binary Search / Divide and Conquer

**Cut problem space in half repeatedly.**

**Technique:**
```python
# Large function with bug somewhere
def process_data(data):
    # Step 1
    cleaned = clean_data(data)
    print("After clean:", cleaned)  # Checkpoint

    # Step 2
    validated = validate_data(cleaned)
    print("After validate:", validated)  # Checkpoint

    # Step 3
    transformed = transform_data(validated)
    print("After transform:", transformed)  # Checkpoint

    # Step 4
    result = save_data(transformed)
    print("After save:", result)  # Checkpoint

    return result

# Find which step introduces the bug
# If "After validate" is correct but "After transform" is wrong,
# bug is in transform_data()
```

**Git Bisect for Historical Bugs:**
```bash
# Find which commit broke the feature
git bisect start
git bisect bad                 # Current version is broken
git bisect good v1.0.0        # v1.0.0 worked

# Git will checkout middle commit
# Test the code, then:
git bisect good   # If this commit works
# or
git bisect bad    # If this commit is broken

# Repeat until git identifies the breaking commit
git bisect reset  # When done
```

---

### 4. Rubber Duck Debugging

**Explain the problem out loud to find the solution.**

**Why It Works:**
- Forces you to articulate the problem clearly
- Often reveals assumptions you're making
- Engages different part of brain
- Slows you down to think carefully

**Process:**
1. Get a rubber duck (or colleague)
2. Explain what the code is supposed to do
3. Walk through the code line by line
4. Explain what each line does
5. Often you'll spot the bug while explaining

**Example:**
```
"This function is supposed to find the maximum value in an array.
First, I initialize max to the first element... wait.
What if the array is empty? I should check that first!"
```

---

### 5. Scientific Method

**Treat debugging like a science experiment.**

**Steps:**
1. **Observe**: What is happening?
2. **Hypothesize**: Why might this happen?
3. **Predict**: If my hypothesis is correct, what should I observe?
4. **Experiment**: Test the hypothesis
5. **Analyze**: Did results match prediction?
6. **Conclude**: Was hypothesis correct? If not, form new hypothesis

**Example:**
```python
# Observation: API returns 500 error intermittently

# Hypothesis: Race condition in concurrent requests

# Prediction: Error should be more frequent under high load

# Experiment:
import concurrent.futures

def load_test():
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(make_request) for _ in range(100)]
        errors = sum(1 for f in futures if f.result().status_code == 500)
    return errors

# Analysis: 45/100 requests failed under load
# Conclusion: Hypothesis confirmed - likely race condition
```

---

## Advanced Debugging Techniques

### 6. Time-Travel Debugging

**Step backward through execution to understand how state evolved.**

**Tools:**
- **rr (Record and Replay)**: Record execution, replay with full control
- **GDB with reverse debugging**: Step backward in C/C++
- **JavaScript Time Travel**: Chrome DevTools

**Example:**
```bash
# Record execution
rr record python app.py

# Replay with debugging
rr replay
(rr) break calculate_total
(rr) continue
(rr) reverse-continue  # Go backward!
(rr) reverse-step      # Step backward
```

---

### 7. Differential Debugging

**Compare working vs. broken to find difference.**

**Scenarios:**
- Works in dev, fails in production
- Works for some users, fails for others
- Worked in v1.0, broken in v2.0

**Process:**
```python
# Compare environments
def debug_environment():
    print("Python version:", sys.version)
    print("Dependencies:", [f"{pkg}=={version}" for pkg, version in packages])
    print("Environment vars:", os.environ)
    print("Config:", config.to_dict())

# Log in both environments, diff the output
```

**Example:**
```bash
# Works in dev (Python 3.9)
Python 3.9: dict maintains insertion order

# Fails in production (Python 3.6)
Python 3.6: dict order not guaranteed

# Bug: Code relies on dict order
```

---

### 8. Minimal Reproducible Example

**Reduce to smallest code that demonstrates the bug.**

**Benefits:**
- Easier to understand
- Faster to debug
- Eliminates irrelevant code
- Can be shared for help

**Example:**
```python
# Original: 500 lines, bug somewhere
# Reduced:

# Minimal example that reproduces the bug
def process(data):
    return data.split(',')  # Bug: assumes string

# Bug: TypeError if data is list
process(['a', 'b'])  # Fails

# Now obvious: Need type check
def process(data):
    if isinstance(data, list):
        return data
    return data.split(',')
```

---

### 9. Logging Strategy

**Strategic logging reveals execution flow.**

**Logging Levels:**
```python
import logging

# DEBUG: Detailed diagnostic information
logging.debug(f"User {user_id} requesting order {order_id}")

# INFO: Confirmation things are working
logging.info(f"Order {order_id} created successfully")

# WARNING: Something unexpected but handled
logging.warning(f"Customer {customer_id} not found, using default")

# ERROR: Error occurred, function failed
logging.error(f"Failed to process order {order_id}: {error}")

# CRITICAL: Serious error, application may crash
logging.critical(f"Database connection lost")
```

**Structured Logging:**
```python
# Include context in logs
logging.info("Order processed", extra={
    'order_id': order.id,
    'user_id': user.id,
    'amount': order.total,
    'duration_ms': duration
})

# Easy to search/filter in log aggregation tools
```

---

### 10. Breakpoint Debugging

**Pause execution and inspect state.**

**Python (pdb):**
```python
import pdb

def calculate_total(items):
    total = 0
    for item in items:
        pdb.set_trace()  # Pause here
        total += item.price * item.quantity
    return total

# When paused:
# (pdb) print(item)        # Inspect variables
# (pdb) print(total)
# (pdb) next               # Next line
# (pdb) continue           # Resume
# (pdb) quit               # Exit
```

**JavaScript (Chrome DevTools):**
```javascript
function processOrder(order) {
    debugger;  // Pause here
    const total = calculateTotal(order.items);
    return total;
}

// Browser will pause at debugger statement
// Inspect variables in DevTools Console
```

**Conditional Breakpoints:**
```python
# Only break when condition is true
if user_id == 123:
    import pdb; pdb.set_trace()
```

---

## Debugging Patterns by Error Type

### Null/Undefined Errors

**Strategy: Trace backward to find where None/null was introduced**

```python
# Error: AttributeError: 'NoneType' object has no attribute 'name'
user.name

# Debug: Trace backward
user = get_user(user_id)  # Returns None - why?
  └─ user = db.query(User).get(user_id)  # user_id doesn't exist
     └─ user_id = request.args.get('id')  # Missing from request

# Fix: Validate at each step
user_id = request.args.get('id')
if not user_id:
    return error("Missing user_id"), 400

user = get_user(user_id)
if not user:
    return error("User not found"), 404
```

---

### Off-By-One Errors

**Strategy: Check boundary conditions**

```python
# Bug: Last item missing
def process_items(items):
    for i in range(len(items) - 1):  # Bug: stops at len-1
        print(items[i])

# Debug: Test with small input
process_items([1, 2, 3])
# Prints: 1, 2 (missing 3!)

# Fix: Remove -1
def process_items(items):
    for i in range(len(items)):
        print(items[i])

# Or use cleaner iteration
def process_items(items):
    for item in items:
        print(item)
```

---

### Race Conditions

**Strategy: Reduce concurrency to isolate**

```python
# Bug: Intermittent wrong results

# Debug: Test with concurrency=1
results_single = run_with_workers(1)  # Works
results_multi = run_with_workers(10)  # Fails

# Hypothesis: Race condition

# Identify shared state
shared_counter = 0  # Suspicious!

def increment():
    global shared_counter
    shared_counter += 1  # Not thread-safe!

# Fix: Add synchronization
import threading
lock = threading.Lock()

def increment():
    with lock:
        global shared_counter
        shared_counter += 1
```

---

### Memory Leaks

**Strategy: Monitor memory over time**

```python
import tracemalloc
import gc

# Start tracking
tracemalloc.start()

# Snapshot before
snapshot1 = tracemalloc.take_snapshot()

# Run code
for _ in range(1000):
    leak_function()

# Force garbage collection
gc.collect()

# Snapshot after
snapshot2 = tracemalloc.take_snapshot()

# Compare
top_stats = snapshot2.compare_to(snapshot1, 'lineno')

for stat in top_stats[:10]:
    print(f"{stat.size_diff / 1024:.1f} KB: {stat}")

# Shows which lines are allocating memory
```

---

## Debugging Checklist

### Before You Start
- [ ] Can you reproduce the bug consistently?
- [ ] Do you have the full error message and stack trace?
- [ ] Do you understand what the code is supposed to do?
- [ ] Have you checked for recent changes (git blame/log)?

### Investigation
- [ ] Read the error message carefully
- [ ] Analyze the stack trace
- [ ] Form hypothesis about cause
- [ ] Add logging/breakpoints to test hypothesis
- [ ] Verify assumptions about data/state

### Resolution
- [ ] Understand root cause (not just symptom)
- [ ] Implement fix based on understanding
- [ ] Test fix thoroughly
- [ ] Add test case to prevent regression
- [ ] Document the bug and fix

---

## Summary

**Effective debugging requires:**
1. **Systematic approach**: Follow a method
2. **Patience**: Don't guess randomly
3. **Understanding**: Know what the code does
4. **Observation**: Pay attention to details
5. **Documentation**: Record findings
6. **Testing**: Verify fixes work
7. **Learning**: Understand why it happened

**Remember**: Every bug is an opportunity to improve your understanding of the system!
