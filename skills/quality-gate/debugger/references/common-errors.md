# Common Error Patterns by Language

Language-specific error patterns, causes, and solutions.

## Python Common Errors

### AttributeError: 'NoneType' object has no attribute 'X'

**Cause**: Trying to access attribute on None

**Example:**
```python
user = get_user(user_id)  # Returns None
print(user.name)  # AttributeError

# Common scenarios:
result = dict.get('key')  # Returns None if key missing
result.value  # Error!

data = re.search(pattern, text)  # Returns None if no match
data.group(1)  # Error!
```

**Solution:**
```python
# Check for None
user = get_user(user_id)
if user is None:
    handle_error()
print(user.name)

# Or use get() with default
value = dict.get('key', default_value)

# Or use optional chaining (Python 3.8+)
name = user.name if user else None
```

---

### KeyError: 'key'

**Cause**: Dictionary key doesn't exist

**Example:**
```python
config = {'host': 'localhost'}
port = config['port']  # KeyError: 'port'
```

**Solution:**
```python
# Use get() with default
port = config.get('port', 8080)

# Or check first
if 'port' in config:
    port = config['port']

# Or use try/except
try:
    port = config['port']
except KeyError:
    port = 8080
```

---

### IndexError: list index out of range

**Cause**: Accessing index that doesn't exist

**Example:**
```python
items = [1, 2, 3]
fourth = items[3]  # IndexError (valid indices: 0, 1, 2)
```

**Solution:**
```python
# Check length
if len(items) > 3:
    fourth = items[3]

# Or use try/except
try:
    fourth = items[3]
except IndexError:
    fourth = None

# Or use slice (never raises error)
fourth = items[3:4]  # Returns [] if out of range
```

---

### TypeError: unsupported operand type(s)

**Cause**: Operation on incompatible types

**Example:**
```python
result = "5" + 3  # TypeError: can't concatenate str and int

# Common scenarios:
total = sum(['1', '2', '3'])  # TypeError: unsupported operand type(s) for +: 'int' and 'str'
```

**Solution:**
```python
# Convert types
result = int("5") + 3  # 8
result = "5" + str(3)  # "53"

# Validate types
def add(a, b):
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Arguments must be numbers")
    return a + b
```

---

### IndentationError: unexpected indent

**Cause**: Inconsistent indentation (spaces vs tabs)

**Example:**
```python
def my_function():
    print("hello")
        print("world")  # IndentationError
```

**Solution:**
```python
# Use consistent indentation (4 spaces is standard)
def my_function():
    print("hello")
    print("world")

# Configure editor to use spaces, not tabs
```

---

## JavaScript Common Errors

### TypeError: Cannot read property 'X' of undefined

**Cause**: Accessing property on undefined

**Example:**
```javascript
const user = getUser(userId);  // Returns undefined
console.log(user.name);  // TypeError

// Common scenarios:
const data = JSON.parse(response);
console.log(data.user.profile.name);  // Error if any level is undefined
```

**Solution:**
```javascript
// Check for undefined
const user = getUser(userId);
if (user) {
    console.log(user.name);
}

// Optional chaining (ES2020)
console.log(user?.name);  // Returns undefined if user is undefined

// Nested optional chaining
console.log(data?.user?.profile?.name);

// Nullish coalescing with default
const name = user?.name ?? 'Unknown';
```

---

### ReferenceError: X is not defined

**Cause**: Variable used before declaration

**Example:**
```javascript
console.log(myVar);  // ReferenceError
const myVar = 10;

// Or typo in variable name
let userName = 'John';
console.log(usrName);  // ReferenceError (typo)
```

**Solution:**
```javascript
// Declare before use
const myVar = 10;
console.log(myVar);

// Use strict mode to catch errors
'use strict';

// Check variable name spelling
```

---

### TypeError: X is not a function

**Cause**: Trying to call something that's not a function

**Example:**
```javascript
const data = { name: 'John' };
data.forEach(item => console.log(item));  // TypeError: data.forEach is not a function

// Common scenarios:
const result = getSomething();  // Returns null/undefined
result();  // TypeError
```

**Solution:**
```javascript
// Check type before calling
if (typeof data.forEach === 'function') {
    data.forEach(item => console.log(item));
}

// Or check if it's an array
if (Array.isArray(data)) {
    data.forEach(item => console.log(item));
}

// Validate return value
const result = getSomething();
if (typeof result === 'function') {
    result();
}
```

---

### UnhandledPromiseRejectionWarning

**Cause**: Promise rejection not caught

**Example:**
```javascript
async function fetchData() {
    const response = await fetch('/api/data');  // Network error
    return response.json();  // Never reached if fetch fails
}

fetchData();  // UnhandledPromiseRejectionWarning if fetch fails
```

**Solution:**
```javascript
// Use try/catch with async/await
async function fetchData() {
    try {
        const response = await fetch('/api/data');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error('Error fetching data:', error);
        throw error;
    }
}

// Or use .catch()
fetchData().catch(error => {
    console.error('Error:', error);
});
```

---

### RangeError: Maximum call stack size exceeded

**Cause**: Infinite recursion

**Example:**
```javascript
function factorial(n) {
    return n * factorial(n - 1);  // Missing base case!
}

factorial(5);  // RangeError
```

**Solution:**
```javascript
// Add base case
function factorial(n) {
    if (n <= 1) return 1;  // Base case
    return n * factorial(n - 1);
}

// Or use iteration
function factorial(n) {
    let result = 1;
    for (let i = 2; i <= n; i++) {
        result *= i;
    }
    return result;
}
```

---

## Java Common Errors

### NullPointerException

**Cause**: Accessing method/field on null object

**Example:**
```java
User user = userRepository.findById(userId);  // Returns null
String name = user.getName();  // NullPointerException

// Common scenarios:
List<String> list = getList();
list.add("item");  // NPE if getList() returns null
```

**Solution:**
```java
// Check for null
User user = userRepository.findById(userId);
if (user != null) {
    String name = user.getName();
}

// Use Optional (Java 8+)
Optional<User> userOpt = userRepository.findById(userId);
String name = userOpt.map(User::getName)
                     .orElse("Unknown");

// Objects.requireNonNull for validation
public void setName(String name) {
    this.name = Objects.requireNonNull(name, "Name cannot be null");
}
```

---

### ClassCastException

**Cause**: Invalid type casting

**Example:**
```java
Object obj = "Hello";
Integer num = (Integer) obj;  // ClassCastException

// Common with collections
List<Object> list = Arrays.asList("a", "b", 1);
for (Object item : list) {
    String str = (String) item;  // ClassCastException on 3rd iteration
}
```

**Solution:**
```java
// Check type before casting
if (obj instanceof Integer) {
    Integer num = (Integer) obj;
}

// Use generics
List<String> list = new ArrayList<>();  // Type-safe

// Pattern matching (Java 16+)
if (obj instanceof String str) {
    System.out.println(str.toUpperCase());
}
```

---

### ConcurrentModificationException

**Cause**: Modifying collection while iterating

**Example:**
```java
List<String> list = new ArrayList<>(Arrays.asList("a", "b", "c"));
for (String item : list) {
    if (item.equals("b")) {
        list.remove(item);  // ConcurrentModificationException
    }
}
```

**Solution:**
```java
// Use Iterator.remove()
Iterator<String> iterator = list.iterator();
while (iterator.hasNext()) {
    String item = iterator.next();
    if (item.equals("b")) {
        iterator.remove();  // Safe
    }
}

// Or removeIf (Java 8+)
list.removeIf(item -> item.equals("b"));

// Or collect to new list
List<String> filtered = list.stream()
    .filter(item -> !item.equals("b"))
    .collect(Collectors.toList());
```

---

### OutOfMemoryError: Java heap space

**Cause**: Running out of heap memory

**Example:**
```java
// Memory leak: unbounded cache
Map<String, byte[]> cache = new HashMap<>();

public void cacheData(String key) {
    cache.put(key, new byte[1_000_000]);  // 1MB each
    // Never removed - eventually OutOfMemoryError
}
```

**Solution:**
```java
// Use bounded cache
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

Cache<String, byte[]> cache = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build();

// Increase heap size
// java -Xmx2G MyApp  // 2GB heap

// Profile memory usage
// jmap -heap <pid>
// jmap -histo <pid>
```

---

### StackOverflowError

**Cause**: Infinite recursion or very deep call stack

**Example:**
```java
public void recursiveMethod() {
    recursiveMethod();  // No base case!
}
```

**Solution:**
```java
// Add base case
public int factorial(int n) {
    if (n <= 1) return 1;  // Base case
    return n * factorial(n - 1);
}

// Or use iteration
public int factorial(int n) {
    int result = 1;
    for (int i = 2; i <= n; i++) {
        result *= i;
    }
    return result;
}

// Increase stack size if needed
// java -Xss2m MyApp  // 2MB stack
```

---

## SQL Common Errors

### Syntax Error near 'X'

**Cause**: Invalid SQL syntax

**Example:**
```sql
-- Missing comma
SELECT id name email FROM users;  -- Syntax error

-- Reserved keyword without quotes
SELECT order FROM orders;  -- 'order' is reserved

-- Missing FROM clause
SELECT *;  -- Syntax error
```

**Solution:**
```sql
-- Add comma
SELECT id, name, email FROM users;

-- Quote reserved keywords
SELECT "order" FROM orders;
-- Or rename column
SELECT order_number FROM orders;

-- Include FROM clause
SELECT * FROM users;
```

---

### Duplicate Key Error

**Cause**: Violating unique constraint

**Example:**
```sql
INSERT INTO users (id, email) VALUES (1, 'user@example.com');
INSERT INTO users (id, email) VALUES (1, 'other@example.com');
-- Error: Duplicate entry '1' for key 'PRIMARY'
```

**Solution:**
```sql
-- Use different ID (or auto-increment)
INSERT INTO users (email) VALUES ('user@example.com');
-- ID auto-generated

-- Or UPDATE if exists
INSERT INTO users (id, email) VALUES (1, 'user@example.com')
ON DUPLICATE KEY UPDATE email = 'user@example.com';

-- Or check before insert
INSERT INTO users (id, email)
SELECT 1, 'user@example.com'
WHERE NOT EXISTS (SELECT 1 FROM users WHERE id = 1);
```

---

## Debugging Quick Reference

### Python
```python
# Print debugging
print(f"Variable: {variable}")

# Debugger
import pdb; pdb.set_trace()

# Logging
import logging
logging.debug("Debug message")
```

### JavaScript
```javascript
// Print debugging
console.log("Variable:", variable);
console.table(array);  // Table format

// Debugger
debugger;

// Stack trace
console.trace("Trace");
```

### Java
```java
// Print debugging
System.out.println("Variable: " + variable);

// Logging
logger.debug("Debug message");

// Stack trace
Thread.dumpStack();
```

---

## Error Pattern Summary

| Error Type | Common Cause | Quick Fix |
|------------|--------------|-----------|
| NullPointerException / TypeError | Accessing null/undefined | Check for null before access |
| IndexError / ArrayIndexOutOfBounds | Index out of range | Check array length |
| KeyError / undefined | Missing key | Use get() with default |
| TypeError / ClassCastException | Type mismatch | Validate types |
| StackOverflow / Maximum call stack | Infinite recursion | Add base case |
| Memory errors | Memory leak | Limit cache size, fix leaks |
| Syntax errors | Typos, wrong syntax | Read error message carefully |
| Concurrency errors | Race conditions | Add synchronization |

**Remember**: Read error messages carefully - they usually tell you exactly what's wrong!
