# Performance Optimization Patterns

Common patterns and techniques for optimizing code performance across different languages and scenarios.

## Algorithm Optimization Patterns

### Pattern 1: Hash Table Lookup (O(n) → O(1))

**Problem**: Linear search in collection

**Python:**
```python
# Inefficient: O(n) lookup
def has_permission(user_id, permissions_list):
    for permission in permissions_list:
        if permission.user_id == user_id:
            return True
    return False

# Optimized: O(1) lookup
def has_permission(user_id, permissions_dict):
    return user_id in permissions_dict

# Build the dict once
permissions_dict = {p.user_id: p for p in permissions_list}
```

**JavaScript:**
```javascript
// Inefficient: O(n) with Array.includes
const hasPermission = (userId, permissionsArray) => {
    return permissionsArray.includes(userId);
};

// Optimized: O(1) with Set
const permissionsSet = new Set(permissionsArray);
const hasPermission = (userId, permissionsSet) => {
    return permissionsSet.has(userId);
};
```

**Java:**
```java
// Inefficient: O(n) with ArrayList
public boolean hasPermission(Long userId, List<Permission> permissions) {
    for (Permission p : permissions) {
        if (p.getUserId().equals(userId)) {
            return true;
        }
    }
    return false;
}

// Optimized: O(1) with HashSet
private Set<Long> permissionSet = new HashSet<>(permissions.stream()
    .map(Permission::getUserId)
    .collect(Collectors.toSet()));

public boolean hasPermission(Long userId) {
    return permissionSet.contains(userId);
}
```

**Improvement**: O(n) → O(1) lookup time

---

### Pattern 2: Memoization/Caching

**Problem**: Expensive repeated calculations

**Python:**
```python
# Inefficient: Recalculates every time
def fibonacci(n):
    if n <= 1:
        return n
    return fibonacci(n-1) + fibonacci(n-2)  # O(2^n) exponential!

# Optimized: Memoization
from functools import lru_cache

@lru_cache(maxsize=None)
def fibonacci(n):
    if n <= 1:
        return n
    return fibonacci(n-1) + fibonacci(n-2)  # O(n) with caching

# Or manual caching
_fib_cache = {}
def fibonacci(n):
    if n in _fib_cache:
        return _fib_cache[n]
    if n <= 1:
        result = n
    else:
        result = fibonacci(n-1) + fibonacci(n-2)
    _fib_cache[n] = result
    return result
```

**JavaScript:**
```javascript
// Inefficient: No caching
function expensiveComputation(data) {
    // Complex calculation
    return result;
}

// Optimized: Memoization
const cache = new Map();

function expensiveComputation(data) {
    const key = JSON.stringify(data);
    if (cache.has(key)) {
        return cache.get(key);
    }

    // Complex calculation
    const result = /* ... */;
    cache.set(key, result);
    return result;
}

// React useMemo example
import { useMemo } from 'react';

function Component({ data }) {
    const computed = useMemo(
        () => expensiveComputation(data),
        [data]  // Only recalculate when data changes
    );
}
```

**Java:**
```java
// Inefficient: No caching
public class Calculator {
    public BigDecimal expensiveComputation(Data data) {
        // Complex calculation
        return result;
    }
}

// Optimized: Caffeine cache
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class Calculator {
    private final Cache<Data, BigDecimal> cache = Caffeine.newBuilder()
        .maximumSize(1000)
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .build();

    public BigDecimal expensiveComputation(Data data) {
        return cache.get(data, key -> {
            // Complex calculation only if not cached
            return calculateResult(key);
        });
    }
}
```

---

### Pattern 3: Batch Processing

**Problem**: Multiple individual operations

**Python:**
```python
# Inefficient: Individual database operations
def update_users(user_ids, status):
    for user_id in user_ids:
        db.execute(
            "UPDATE users SET status = ? WHERE id = ?",
            (status, user_id)
        )
    db.commit()

# Optimized: Batch operation
def update_users(user_ids, status):
    db.execute(
        "UPDATE users SET status = ? WHERE id IN (?)",
        (status, ','.join(str(id) for id in user_ids))
    )
    db.commit()

# Or with ORM
User.query.filter(User.id.in_(user_ids)).update(
    {'status': status},
    synchronize_session=False
)
db.session.commit()
```

**JavaScript:**
```javascript
// Inefficient: Individual API calls
async function updateUsers(userIds, status) {
    for (const userId of userIds) {
        await fetch(`/api/users/${userId}`, {
            method: 'PATCH',
            body: JSON.stringify({ status })
        });
    }
}

// Optimized: Batch API call
async function updateUsers(userIds, status) {
    await fetch('/api/users/batch', {
        method: 'PATCH',
        body: JSON.stringify({
            user_ids: userIds,
            status: status
        })
    });
}

// Or parallel execution
async function updateUsers(userIds, status) {
    await Promise.all(
        userIds.map(userId =>
            fetch(`/api/users/${userId}`, {
                method: 'PATCH',
                body: JSON.stringify({ status })
            })
        )
    );
}
```

**Java:**
```java
// Inefficient: Individual database operations
public void updateUsers(List<Long> userIds, String status) {
    for (Long userId : userIds) {
        jdbcTemplate.update(
            "UPDATE users SET status = ? WHERE id = ?",
            status, userId
        );
    }
}

// Optimized: Batch update
public void updateUsers(List<Long> userIds, String status) {
    String sql = "UPDATE users SET status = ? WHERE id = ?";
    jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
            ps.setString(1, status);
            ps.setLong(2, userIds.get(i));
        }

        @Override
        public int getBatchSize() {
            return userIds.size();
        }
    });
}

// Or with JPA
@Query("UPDATE User u SET u.status = :status WHERE u.id IN :ids")
@Modifying
void updateUserStatuses(@Param("status") String status, @Param("ids") List<Long> ids);
```

---

### Pattern 4: Lazy Loading / Pagination

**Problem**: Loading too much data at once

**Python:**
```python
# Inefficient: Load all records
def get_all_users():
    return User.query.all()  # Could be millions of records

# Optimized: Pagination
def get_users_page(page=1, per_page=100):
    return User.query.paginate(
        page=page,
        per_page=per_page,
        error_out=False
    )

# Or use generator for large datasets
def get_users_lazy():
    return User.query.yield_per(1000)  # Fetch in batches of 1000

for user in get_users_lazy():
    process_user(user)
```

**JavaScript:**
```javascript
// Inefficient: Load all data
const getAllProducts = async () => {
    const products = await Product.findAll();  // Could be millions
    return products;
};

// Optimized: Pagination
const getProductsPage = async (page = 1, limit = 100) => {
    const offset = (page - 1) * limit;
    const products = await Product.findAll({
        limit: limit,
        offset: offset
    });
    return products;
};

// Cursor-based pagination (better for large datasets)
const getProductsAfter = async (cursor, limit = 100) => {
    const products = await Product.findAll({
        where: {
            id: { $gt: cursor }
        },
        limit: limit,
        order: [['id', 'ASC']]
    });
    return products;
};
```

**Java:**
```java
// Inefficient: Load all entities
public List<Product> getAllProducts() {
    return productRepository.findAll();  // Loads everything into memory
}

// Optimized: Pageable
public Page<Product> getProducts(int page, int size) {
    Pageable pageable = PageRequest.of(page, size);
    return productRepository.findAll(pageable);
}

// Stream for processing large datasets
public void processAllProducts() {
    try (Stream<Product> stream = productRepository.streamAll()) {
        stream.forEach(this::processProduct);
    }
}

@Query("SELECT p FROM Product p")
Stream<Product> streamAll();
```

---

### Pattern 5: Debouncing / Throttling

**Problem**: Too many executions of expensive operations

**JavaScript:**
```javascript
// Inefficient: Executes on every keystroke
function handleSearch(event) {
    const query = event.target.value;
    fetch(`/api/search?q=${query}`);  // API call on every keystroke
}

input.addEventListener('input', handleSearch);

// Optimized: Debounce (wait for pause in input)
function debounce(func, delay) {
    let timeoutId;
    return function(...args) {
        clearTimeout(timeoutId);
        timeoutId = setTimeout(() => func.apply(this, args), delay);
    };
}

const debouncedSearch = debounce((query) => {
    fetch(`/api/search?q=${query}`);
}, 300);  // Wait 300ms after last keystroke

input.addEventListener('input', (e) => {
    debouncedSearch(e.target.value);
});

// Throttle (limit execution frequency)
function throttle(func, limit) {
    let inThrottle;
    return function(...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

const throttledScroll = throttle(() => {
    console.log('Scroll event');
}, 100);  // Max once per 100ms

window.addEventListener('scroll', throttledScroll);
```

**Python:**
```python
import time
from functools import wraps

# Throttle decorator
def throttle(seconds):
    def decorator(func):
        last_called = [0]

        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            if now - last_called[0] >= seconds:
                last_called[0] = now
                return func(*args, **kwargs)
        return wrapper
    return decorator

@throttle(1.0)  # Max once per second
def expensive_operation():
    # API call or expensive computation
    pass
```

---

### Pattern 6: Connection Pooling

**Problem**: Creating new connections for each request

**Python:**
```python
# Inefficient: New connection per request
def get_user(user_id):
    conn = psycopg2.connect(dbname='mydb', user='user', password='pass')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result

# Optimized: Connection pool
from psycopg2 import pool

connection_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dbname='mydb',
    user='user',
    password='pass'
)

def get_user(user_id):
    conn = connection_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        return cursor.fetchone()
    finally:
        connection_pool.putconn(conn)
```

**JavaScript:**
```javascript
// Inefficient: New connection per query
const mysql = require('mysql');

async function getUser(userId) {
    const connection = mysql.createConnection({
        host: 'localhost',
        user: 'user',
        password: 'password',
        database: 'mydb'
    });

    connection.connect();
    const result = await query(connection, 'SELECT * FROM users WHERE id = ?', [userId]);
    connection.end();
    return result;
}

// Optimized: Connection pool
const pool = mysql.createPool({
    host: 'localhost',
    user: 'user',
    password: 'password',
    database: 'mydb',
    connectionLimit: 10
});

async function getUser(userId) {
    return new Promise((resolve, reject) => {
        pool.query('SELECT * FROM users WHERE id = ?', [userId], (err, results) => {
            if (err) reject(err);
            else resolve(results[0]);
        });
    });
}
```

**Java:**
```java
// Inefficient: New connection per request
public User getUser(Long userId) throws SQLException {
    Connection conn = DriverManager.getConnection(
        "jdbc:mysql://localhost/mydb", "user", "password");
    PreparedStatement stmt = conn.prepareStatement(
        "SELECT * FROM users WHERE id = ?");
    stmt.setLong(1, userId);
    ResultSet rs = stmt.executeQuery();
    // ... process result
    conn.close();
    return user;
}

// Optimized: HikariCP connection pool
@Configuration
public class DataSourceConfig {
    @Bean
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost/mydb");
        config.setUsername("user");
        config.setPassword("password");
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }
}
```

---

### Pattern 7: Async I/O

**Problem**: Blocking I/O operations

**Python:**
```python
# Inefficient: Synchronous I/O
import requests

def fetch_all_urls(urls):
    results = []
    for url in urls:
        response = requests.get(url)  # Blocks for each request
        results.append(response.json())
    return results

# 10 URLs × 200ms = 2 seconds

# Optimized: Async I/O
import asyncio
import aiohttp

async def fetch_url(session, url):
    async with session.get(url) as response:
        return await response.json()

async def fetch_all_urls(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
    return results

# 10 URLs in parallel = ~200ms (10x faster)
```

**JavaScript:**
```javascript
// Inefficient: Sequential awaits
async function fetchAllUsers(userIds) {
    const users = [];
    for (const id of userIds) {
        const user = await fetch(`/api/users/${id}`);
        users.push(await user.json());
    }
    return users;
}

// Optimized: Parallel promises
async function fetchAllUsers(userIds) {
    const promises = userIds.map(id =>
        fetch(`/api/users/${id}`).then(r => r.json())
    );
    return await Promise.all(promises);
}
```

**Java:**
```java
// Inefficient: Sequential blocking calls
public List<User> fetchAllUsers(List<Long> userIds) {
    List<User> users = new ArrayList<>();
    for (Long id : userIds) {
        User user = restTemplate.getForObject(
            "/api/users/" + id, User.class);
        users.add(user);
    }
    return users;
}

// Optimized: CompletableFuture for parallel execution
public List<User> fetchAllUsers(List<Long> userIds) {
    List<CompletableFuture<User>> futures = userIds.stream()
        .map(id -> CompletableFuture.supplyAsync(() ->
            restTemplate.getForObject("/api/users/" + id, User.class)
        ))
        .collect(Collectors.toList());

    return futures.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList());
}
```

---

## Summary

Key performance patterns:
1. **Hash Table Lookup**: O(n) → O(1) with Set/Map
2. **Memoization**: Cache expensive computations
3. **Batch Processing**: Reduce round trips
4. **Lazy Loading**: Load data on demand
5. **Debounce/Throttle**: Limit execution frequency
6. **Connection Pooling**: Reuse connections
7. **Async I/O**: Non-blocking operations
8. **Indexing**: Fast database lookups
9. **Data Structure Choice**: Right tool for the job
10. **Algorithm Selection**: Optimal complexity

**Remember**: Always measure before and after optimization!
