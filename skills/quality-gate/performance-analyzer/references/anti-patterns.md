# Performance Anti-Patterns

Common performance mistakes and how to avoid them.

## Database Anti-Patterns

### Anti-Pattern 1: N+1 Query Problem

**Description**: Executing N additional queries in a loop after an initial query.

**Example (Python/SQLAlchemy):**
```python
# BAD: N+1 queries
users = User.query.all()  # 1 query
for user in users:  # N iterations
    print(user.posts)  # N queries (lazy load)

# 100 users = 101 database queries!
```

**Why It's Bad:**
- Database round trips are expensive (network latency)
- Query execution overhead multiplied by N
- Doesn't scale with data size

**Solution:**
```python
# GOOD: Eager loading with 1 query
users = User.query.options(
    joinedload(User.posts)
).all()  # 1 query with JOIN

for user in users:
    print(user.posts)  # No additional query

# 100 users = 1 database query
```

---

### Anti-Pattern 2: SELECT * Without Filtering

**Description**: Fetching all columns and rows when only a subset is needed.

**Example (JavaScript):**
```javascript
// BAD: Fetch everything
const users = await db.query('SELECT * FROM users');
const activeUsers = users.filter(u => u.status === 'active');

// Transferred megabytes of data, processed in application
```

**Why It's Bad:**
- Unnecessary network bandwidth
- Memory overhead
- Application-side filtering is slower than database

**Solution:**
```javascript
// GOOD: Filter in database
const users = await db.query(
    'SELECT id, name, email FROM users WHERE status = ?',
    ['active']
);

// Only transfer needed data
```

---

### Anti-Pattern 3: Missing Indexes

**Description**: Querying columns without indexes, forcing full table scans.

**Example (SQL):**
```sql
-- BAD: No index on email column
SELECT * FROM users WHERE email = 'user@example.com';
-- Full table scan: O(n) time

-- GOOD: Add index
CREATE INDEX idx_users_email ON users(email);
-- Index lookup: O(log n) time
```

**Detection:**
```sql
-- PostgreSQL: Check for sequential scans
EXPLAIN ANALYZE SELECT * FROM users WHERE email = 'user@example.com';

-- Look for "Seq Scan" (bad) vs "Index Scan" (good)
```

---

### Anti-Pattern 4: Unbounded Queries

**Description**: Queries without LIMIT that could return millions of rows.

**Example (Java):**
```java
// BAD: No pagination
@Query("SELECT p FROM Product p")
List<Product> findAll();  // Could be millions of records

// Memory exhausted!
```

**Solution:**
```java
// GOOD: Paginated query
public Page<Product> findAll(Pageable pageable) {
    return productRepository.findAll(pageable);
}

// Usage
Page<Product> products = findAll(PageRequest.of(0, 100));
```

---

## Algorithm Anti-Patterns

### Anti-Pattern 5: Nested Loops on Large Datasets

**Description**: O(n²) or worse complexity on large data.

**Example (Python):**
```python
# BAD: O(n²) nested loops
def find_common_elements(list1, list2):
    common = []
    for item1 in list1:  # O(n)
        for item2 in list2:  # O(n)
            if item1 == item2:
                common.append(item1)
    return common

# 1000 items each = 1,000,000 comparisons
```

**Why It's Bad:**
- Exponential time growth
- 10x more data = 100x slower
- Doesn't scale

**Solution:**
```python
# GOOD: O(n) with set intersection
def find_common_elements(list1, list2):
    return list(set(list1) & set(list2))

# 1000 items each = ~2,000 operations (500x faster)
```

---

### Anti-Pattern 6: Premature Array Copying

**Description**: Copying large arrays unnecessarily.

**Example (JavaScript):**
```javascript
// BAD: Creates new array on each iteration
function processItems(items) {
    let result = [];
    for (let i = 0; i < items.length; i++) {
        result = [...result, items[i] * 2];  // O(n) copy each time
    }
    return result;
}

// Total: O(n²) time due to array copying
```

**Solution:**
```javascript
// GOOD: Mutate array or use push
function processItems(items) {
    const result = [];
    for (let i = 0; i < items.length; i++) {
        result.push(items[i] * 2);  // O(1) append
    }
    return result;
}

// Or use map
function processItems(items) {
    return items.map(item => item * 2);
}
```

---

### Anti-Pattern 7: String Concatenation in Loops

**Description**: Repeatedly concatenating strings (immutable in many languages).

**Example (Java):**
```java
// BAD: O(n²) due to string immutability
String result = "";
for (int i = 0; i < 1000; i++) {
    result += i + ",";  // Creates new string each time
}

// 1000 iterations = ~500,000 character copies
```

**Solution:**
```java
// GOOD: O(n) with StringBuilder
StringBuilder result = new StringBuilder();
for (int i = 0; i < 1000; i++) {
    result.append(i).append(",");
}
String output = result.toString();
```

**Python:**
```python
# BAD
result = ""
for i in range(1000):
    result += str(i) + ","

# GOOD: Use join
result = ",".join(str(i) for i in range(1000))
```

---

## Memory Anti-Patterns

### Anti-Pattern 8: Memory Leaks - Unclosed Resources

**Description**: Not releasing resources (files, connections, event listeners).

**Example (Python):**
```python
# BAD: File not closed on exception
def read_file(filename):
    file = open(filename, 'r')
    data = file.read()
    # If exception occurs, file never closes
    file.close()
    return data
```

**Solution:**
```python
# GOOD: Context manager ensures cleanup
def read_file(filename):
    with open(filename, 'r') as file:
        return file.read()
    # File automatically closed even if exception
```

**JavaScript:**
```javascript
// BAD: Event listener never removed
class Component {
    constructor() {
        window.addEventListener('resize', this.handleResize);
    }
    // Component destroyed but listener still attached
}

// GOOD: Clean up listeners
class Component {
    constructor() {
        this.handleResize = this.handleResize.bind(this);
        window.addEventListener('resize', this.handleResize);
    }

    destroy() {
        window.removeEventListener('resize', this.handleResize);
    }
}
```

---

### Anti-Pattern 9: Loading Everything Into Memory

**Description**: Reading entire large files/datasets into memory.

**Example (Python):**
```python
# BAD: Load 10GB file into memory
with open('huge_file.txt', 'r') as f:
    data = f.read()  # OutOfMemoryError!
    process(data)
```

**Solution:**
```python
# GOOD: Stream line by line
with open('huge_file.txt', 'r') as f:
    for line in f:  # Reads one line at a time
        process(line)

# Or use chunks
with open('huge_file.txt', 'rb') as f:
    while True:
        chunk = f.read(8192)  # 8KB at a time
        if not chunk:
            break
        process(chunk)
```

---

### Anti-Pattern 10: Unbounded Caches

**Description**: Caches that grow without limit.

**Example (JavaScript):**
```javascript
// BAD: Cache grows indefinitely
const cache = {};

function getData(key) {
    if (cache[key]) {
        return cache[key];
    }

    const data = expensiveOperation(key);
    cache[key] = data;  // Never evicted, memory leak!
    return data;
}
```

**Solution:**
```javascript
// GOOD: LRU cache with size limit
const LRU = require('lru-cache');

const cache = new LRU({
    max: 500,  // Maximum 500 items
    maxAge: 1000 * 60 * 60  // 1 hour TTL
});

function getData(key) {
    let data = cache.get(key);
    if (!data) {
        data = expensiveOperation(key);
        cache.set(key, data);
    }
    return data;
}
```

---

## I/O Anti-Patterns

### Anti-Pattern 11: Synchronous I/O in Event Loop

**Description**: Blocking the event loop with synchronous operations.

**Example (Node.js):**
```javascript
// BAD: Blocks event loop
const fs = require('fs');

app.get('/data', (req, res) => {
    const data = fs.readFileSync('large-file.txt');  // Blocks!
    res.send(data);
});

// Server can't handle other requests while reading
```

**Solution:**
```javascript
// GOOD: Async I/O
const fs = require('fs').promises;

app.get('/data', async (req, res) => {
    const data = await fs.readFile('large-file.txt');
    res.send(data);
});

// Or stream for large files
app.get('/data', (req, res) => {
    const stream = fs.createReadStream('large-file.txt');
    stream.pipe(res);
});
```

---

### Anti-Pattern 12: Serial Async Operations

**Description**: Awaiting operations that could run in parallel.

**Example (JavaScript):**
```javascript
// BAD: Sequential - 3 seconds total
async function fetchData() {
    const users = await fetchUsers();     // 1 second
    const posts = await fetchPosts();     // 1 second
    const comments = await fetchComments(); // 1 second
    return { users, posts, comments };
}
```

**Solution:**
```javascript
// GOOD: Parallel - 1 second total
async function fetchData() {
    const [users, posts, comments] = await Promise.all([
        fetchUsers(),
        fetchPosts(),
        fetchComments()
    ]);
    return { users, posts, comments };
}
```

---

## Rendering Anti-Patterns

### Anti-Pattern 13: Unnecessary Re-renders

**Description**: Re-rendering components when data hasn't changed.

**Example (React):**
```javascript
// BAD: Re-renders on every parent update
function ExpensiveComponent({ data }) {
    const result = performExpensiveCalculation(data);
    return <div>{result}</div>;
}

// Parent updates -> child re-renders even if data unchanged
```

**Solution:**
```javascript
// GOOD: Memoize component and calculation
import { memo, useMemo } from 'react';

const ExpensiveComponent = memo(({ data }) => {
    const result = useMemo(
        () => performExpensiveCalculation(data),
        [data]
    );
    return <div>{result}</div>;
});

// Only re-renders when data actually changes
```

---

### Anti-Pattern 14: Rendering Large Lists Without Virtualization

**Description**: Rendering thousands of DOM elements.

**Example (React):**
```javascript
// BAD: Renders 10,000 elements
function UserList({ users }) {
    return (
        <div>
            {users.map(user => (
                <UserCard key={user.id} user={user} />
            ))}
        </div>
    );
}

// 10,000 DOM nodes = slow rendering, sluggish scrolling
```

**Solution:**
```javascript
// GOOD: Virtual scrolling
import { FixedSizeList } from 'react-window';

function UserList({ users }) {
    const Row = ({ index, style }) => (
        <div style={style}>
            <UserCard user={users[index]} />
        </div>
    );

    return (
        <FixedSizeList
            height={600}
            itemCount={users.length}
            itemSize={80}
            width="100%"
        >
            {Row}
        </FixedSizeList>
    );
}

// Only renders ~10 visible items regardless of total
```

---

## Network Anti-Patterns

### Anti-Pattern 15: Chatty API Calls

**Description**: Multiple small API requests instead of one batch request.

**Example (JavaScript):**
```javascript
// BAD: 100 individual API calls
async function loadUsers(userIds) {
    const users = [];
    for (const id of userIds) {
        const response = await fetch(`/api/users/${id}`);
        users.push(await response.json());
    }
    return users;
}

// 100 requests × 20ms latency = 2 seconds minimum
```

**Solution:**
```javascript
// GOOD: Single batch API call
async function loadUsers(userIds) {
    const response = await fetch('/api/users/batch', {
        method: 'POST',
        body: JSON.stringify({ ids: userIds })
    });
    return await response.json();
}

// 1 request × 20ms = 0.02 seconds
```

---

### Anti-Pattern 16: No Request Caching

**Description**: Fetching the same data repeatedly.

**Example (JavaScript):**
```javascript
// BAD: No caching
async function getUserProfile(userId) {
    const response = await fetch(`/api/users/${userId}`);
    return await response.json();
}

// Same user fetched multiple times = wasted requests
```

**Solution:**
```javascript
// GOOD: Cache with TTL
const cache = new Map();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

async function getUserProfile(userId) {
    const cached = cache.get(userId);
    if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
        return cached.data;
    }

    const response = await fetch(`/api/users/${userId}`);
    const data = await response.json();

    cache.set(userId, {
        data,
        timestamp: Date.now()
    });

    return data;
}

// Or use HTTP caching headers
fetch(`/api/users/${userId}`, {
    headers: {
        'Cache-Control': 'max-age=300'  // 5 minutes
    }
});
```

---

## Concurrency Anti-Patterns

### Anti-Pattern 17: Race Conditions

**Description**: Concurrent access to shared state without synchronization.

**Example (Python):**
```python
# BAD: Race condition
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1  # Not atomic!

# Multiple threads incrementing = lost updates
```

**Solution:**
```python
# GOOD: Use thread-safe primitives
from threading import Lock

class Counter:
    def __init__(self):
        self.count = 0
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.count += 1

# Or use atomic types
from threading import atomic_int
count = atomic_int(0)
count.fetch_add(1)  # Atomic increment
```

**Java:**
```java
// BAD: Non-thread-safe
public class Counter {
    private int count = 0;

    public void increment() {
        count++;  // Race condition
    }
}

// GOOD: AtomicInteger
import java.util.concurrent.atomic.AtomicInteger;

public class Counter {
    private AtomicInteger count = new AtomicInteger(0);

    public void increment() {
        count.incrementAndGet();  // Atomic
    }
}
```

---

## Summary Checklist

Avoid these anti-patterns:
- [ ] N+1 queries (use eager loading)
- [ ] SELECT * without filtering
- [ ] Missing database indexes
- [ ] Unbounded queries (use pagination)
- [ ] Nested loops on large data (use hash tables)
- [ ] Array copying in loops
- [ ] String concatenation in loops (use StringBuilder)
- [ ] Unclosed resources (use context managers)
- [ ] Loading large files into memory (use streaming)
- [ ] Unbounded caches (use LRU with limits)
- [ ] Synchronous I/O in event loop
- [ ] Serial async operations (use Promise.all)
- [ ] Unnecessary re-renders (use memoization)
- [ ] Rendering large lists (use virtualization)
- [ ] Chatty API calls (use batching)
- [ ] No request caching
- [ ] Race conditions (use synchronization)

**Remember**: Profile first, then optimize!
