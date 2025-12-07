# Advanced Optimization Techniques

Advanced performance optimization strategies and techniques.

## Algorithmic Optimizations

### Dynamic Programming

**Problem**: Overlapping subproblems solved repeatedly

**Example - Fibonacci (Exponential → Linear):**
```python
# Naive: O(2^n) - exponential
def fib_naive(n):
    if n <= 1:
        return n
    return fib_naive(n-1) + fib_naive(n-2)

# DP Top-Down (Memoization): O(n)
def fib_memo(n, memo={}):
    if n in memo:
        return memo[n]
    if n <= 1:
        return n
    memo[n] = fib_memo(n-1, memo) + fib_memo(n-2, memo)
    return memo[n]

# DP Bottom-Up (Tabulation): O(n), better space
def fib_dp(n):
    if n <= 1:
        return n
    dp = [0] * (n + 1)
    dp[1] = 1
    for i in range(2, n + 1):
        dp[i] = dp[i-1] + dp[i-2]
    return dp[n]

# Space-Optimized: O(n) time, O(1) space
def fib_optimized(n):
    if n <= 1:
        return n
    prev, curr = 0, 1
    for _ in range(2, n + 1):
        prev, curr = curr, prev + curr
    return curr
```

---

### Binary Search

**Replace linear search O(n) with binary search O(log n)**

**JavaScript:**
```javascript
// Linear search: O(n)
function findLinear(arr, target) {
    for (let i = 0; i < arr.length; i++) {
        if (arr[i] === target) return i;
    }
    return -1;
}

// Binary search: O(log n) - requires sorted array
function findBinary(arr, target) {
    let left = 0;
    let right = arr.length - 1;

    while (left <= right) {
        const mid = Math.floor((left + right) / 2);

        if (arr[mid] === target) return mid;
        if (arr[mid] < target) left = mid + 1;
        else right = mid - 1;
    }

    return -1;
}

// 1,000,000 items: 1,000,000 comparisons → 20 comparisons
```

---

### Two Pointers Technique

**Replace nested loops O(n²) with two pointers O(n)**

**Python:**
```python
# Find pair that sums to target
# Naive: O(n²)
def find_pair_naive(arr, target):
    for i in range(len(arr)):
        for j in range(i + 1, len(arr)):
            if arr[i] + arr[j] == target:
                return (arr[i], arr[j])
    return None

# Two pointers: O(n) - requires sorted array
def find_pair_optimized(arr, target):
    arr.sort()  # O(n log n)
    left, right = 0, len(arr) - 1

    while left < right:
        current_sum = arr[left] + arr[right]
        if current_sum == target:
            return (arr[left], arr[right])
        elif current_sum < target:
            left += 1
        else:
            right -= 1

    return None
```

---

## Data Structure Optimizations

### Use Appropriate Data Structures

**Java - Array vs ArrayList vs LinkedList:**
```java
// Random access: Use ArrayList
List<String> list = new ArrayList<>();
list.get(index);  // O(1)

// Frequent insertions/deletions: Use LinkedList
List<String> list = new LinkedList<>();
list.add(0, element);  // O(1) vs O(n) for ArrayList

// Fixed size, primitives: Use arrays
int[] arr = new int[1000];  // Less memory than ArrayList<Integer>

// Lookup by key: Use HashMap
Map<String, User> userMap = new HashMap<>();
userMap.get(key);  // O(1)

// Maintain order: Use LinkedHashMap
Map<String, User> orderedMap = new LinkedHashMap<>();

// Sorted keys: Use TreeMap
Map<String, User> sortedMap = new TreeMap<>();  // O(log n) operations
```

---

### Trie for String Prefix Searches

**Replace linear search with Trie: O(n) → O(k) where k is key length**

**Python:**
```python
class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end = False

class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, word):
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end = True

    def search_prefix(self, prefix):
        node = self.root
        for char in prefix:
            if char not in node.children:
                return []
            node = node.children[char]

        return self._find_words(node, prefix)

    def _find_words(self, node, prefix):
        words = []
        if node.is_end:
            words.append(prefix)

        for char, child in node.children.items():
            words.extend(self._find_words(child, prefix + char))

        return words

# Autocomplete with 10,000 words: O(k) vs O(n*k) for linear search
```

---

## Database Optimizations

### Indexing Strategies

**Create indexes on frequently queried columns:**
```sql
-- Before: Full table scan
SELECT * FROM orders WHERE customer_id = 123;  -- O(n)

-- After: Create index
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
SELECT * FROM orders WHERE customer_id = 123;  -- O(log n)

-- Composite index for multiple columns
CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);

-- Covering index (includes all SELECT columns)
CREATE INDEX idx_orders_covering ON orders(customer_id, order_date, total);
```

---

### Query Optimization

**Python (SQLAlchemy):**
```python
# Inefficient: N+1 queries
users = User.query.all()
for user in users:
    print(user.posts)  # Lazy load - N queries

# Optimized: Eager loading - 1 query
users = User.query.options(joinedload(User.posts)).all()

# Optimized: Select specific columns
users = db.session.query(User.id, User.name).all()

# Optimized: Pagination
users = User.query.paginate(page=1, per_page=100)

# Optimized: Bulk operations
db.session.bulk_insert_mappings(User, user_dicts)
```

---

## Parallel Processing

### Multi-threading for I/O-Bound Tasks

**Python:**
```python
import concurrent.futures
import requests

# Sequential: 10 URLs × 200ms = 2 seconds
def fetch_sequential(urls):
    results = []
    for url in urls:
        response = requests.get(url)
        results.append(response.json())
    return results

# Parallel: 10 URLs in ~200ms (10x faster)
def fetch_parallel(urls):
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(requests.get, url) for url in urls]
        results = [f.result().json() for f in concurrent.futures.as_completed(futures)]
    return results
```

---

### Multi-processing for CPU-Bound Tasks

**Python:**
```python
from multiprocessing import Pool
import numpy as np

# CPU-intensive task
def process_chunk(data):
    return np.sum(data ** 2)

# Sequential: 4 chunks × 1s = 4 seconds
def process_sequential(chunks):
    return [process_chunk(chunk) for chunk in chunks]

# Parallel: 4 chunks on 4 cores = 1 second (4x faster)
def process_parallel(chunks):
    with Pool(processes=4) as pool:
        results = pool.map(process_chunk, chunks)
    return results
```

---

**JavaScript - Worker Threads:**
```javascript
const { Worker } = require('worker_threads');

// CPU-intensive in separate thread
function runWorker(workerData) {
    return new Promise((resolve, reject) => {
        const worker = new Worker('./worker.js', { workerData });
        worker.on('message', resolve);
        worker.on('error', reject);
    });
}

// Process in parallel
async function processParallel(dataChunks) {
    const promises = dataChunks.map(chunk => runWorker(chunk));
    return await Promise.all(promises);
}
```

---

## Memory Optimizations

### Object Pooling

**Reuse objects instead of creating new ones**

**Java:**
```java
// Object pool for expensive objects
public class ConnectionPool {
    private final Queue<Connection> pool = new ConcurrentLinkedQueue<>();
    private final int maxSize;

    public ConnectionPool(int maxSize) {
        this.maxSize = maxSize;
        // Pre-create connections
        for (int i = 0; i < maxSize; i++) {
            pool.offer(createConnection());
        }
    }

    public Connection acquire() {
        Connection conn = pool.poll();
        return conn != null ? conn : createConnection();
    }

    public void release(Connection conn) {
        if (pool.size() < maxSize) {
            pool.offer(conn);
        } else {
            conn.close();
        }
    }

    private Connection createConnection() {
        // Expensive operation
        return new Connection();
    }
}
```

---

### Lazy Initialization

**Delay object creation until needed**

**Python:**
```python
class DataLoader:
    def __init__(self):
        self._heavy_resource = None

    @property
    def heavy_resource(self):
        if self._heavy_resource is None:
            print("Loading heavy resource...")
            self._heavy_resource = self._load_resource()
        return self._heavy_resource

    def _load_resource(self):
        # Expensive initialization
        return ExpensiveResource()
```

---

## Network Optimizations

### HTTP/2 Server Push

**Push resources before client requests them**

**Node.js:**
```javascript
const http2 = require('http2');
const fs = require('fs');

const server = http2.createSecureServer({
    key: fs.readFileSync('key.pem'),
    cert: fs.readFileSync('cert.pem')
});

server.on('stream', (stream, headers) => {
    if (headers[':path'] === '/') {
        // Push CSS before client requests it
        stream.pushStream({ ':path': '/style.css' }, (err, pushStream) => {
            pushStream.respondWithFile('style.css');
        });

        // Push JS
        stream.pushStream({ ':path': '/app.js' }, (err, pushStream) => {
            pushStream.respondWithFile('app.js');
        });

        stream.respondWithFile('index.html');
    }
});
```

---

### Request Batching

**Batch multiple requests into one**

**GraphQL DataLoader:**
```javascript
const DataLoader = require('dataloader');

// Batches requests within single event loop tick
const userLoader = new DataLoader(async (userIds) => {
    // Single database query for all IDs
    const users = await User.findAll({
        where: { id: userIds }
    });

    // Return in same order as requested
    return userIds.map(id => users.find(u => u.id === id));
});

// Usage - these are batched automatically
const user1 = await userLoader.load(1);
const user2 = await userLoader.load(2);
const user3 = await userLoader.load(3);
// Results in 1 query: SELECT * FROM users WHERE id IN (1, 2, 3)
```

---

## Compilation Optimizations

### JIT Compilation (JavaScript V8)

**Tips for better JIT optimization:**
```javascript
// Monomorphic functions (same type) optimize better
function addNumbers(a, b) {
    return a + b;  // Always called with numbers
}

// Avoid:
function add(a, b) {
    return a + b;  // Sometimes numbers, sometimes strings - polymorphic
}

// Use typed arrays for numeric data
const arr = new Float64Array(1000);  // Faster than regular array

// Avoid deleting properties
const obj = { a: 1, b: 2 };
delete obj.a;  // Deoptimizes - creates slow 'dictionary mode'

// Better: Set to null
obj.a = null;

// Avoid sparse arrays
const arr = [1, 2];
arr[1000] = 3;  // Sparse array - slow

// Use same shape for objects
function Point(x, y) {
    this.x = x;
    this.y = y;
    // Always initialize in same order
}
```

---

### Ahead-of-Time Compilation

**Python - Cython:**
```python
# regular_python.py
def calculate_sum(n):
    total = 0
    for i in range(n):
        total += i
    return total

# cython_optimized.pyx
def calculate_sum(int n):
    cdef int total = 0
    cdef int i
    for i in range(n):
        total += i
    return total

# 10-100x faster for numerical code
```

---

## Profiling-Guided Optimization

**Always profile before optimizing:**

**Python:**
```bash
# cProfile
python -m cProfile -o profile.stats script.py

# Analyze results
python -m pstats profile.stats
sort cumtime
stats 10

# Line profiler
kernprof -l -v script.py

# Memory profiler
python -m memory_profiler script.py
```

**JavaScript:**
```bash
# Node.js profiler
node --prof app.js
node --prof-process isolate-*.log > processed.txt

# Chrome DevTools
# Open Performance tab, record, analyze flamegraph
```

**Java:**
```bash
# JProfiler, YourKit, or built-in
java -agentlib:hprof=cpu=samples MyApp

# JFR (Java Flight Recorder)
java -XX:StartFlightRecording=duration=60s,filename=recording.jfr MyApp
```

---

## Summary Checklist

- [ ] Choose optimal algorithm (O(n) vs O(n²))
- [ ] Use appropriate data structures (HashMap vs Array)
- [ ] Add database indexes on query columns
- [ ] Batch database operations
- [ ] Cache expensive computations
- [ ] Use async I/O for I/O-bound tasks
- [ ] Parallelize CPU-intensive tasks
- [ ] Minimize memory allocations
- [ ] Reuse connections (pooling)
- [ ] Profile before optimizing
- [ ] Measure improvement

**Remember**: Correctness > Performance. Optimize after profiling!
