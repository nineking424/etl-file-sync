# Caching Strategies

Comprehensive guide to caching techniques for performance optimization.

## Caching Levels

### 1. Application-Level Caching (In-Memory)

**When to Use**: Expensive computations, frequently accessed data, session data

**Python - functools.lru_cache:**
```python
from functools import lru_cache

@lru_cache(maxsize=128)
def expensive_computation(n):
    # Complex calculation
    return result

# Automatic caching with LRU eviction
```

**JavaScript - Map-based Cache:**
```javascript
const cache = new Map();

function getCachedData(key) {
    if (cache.has(key)) {
        return cache.get(key);
    }

    const data = expensiveOperation(key);
    cache.set(key, data);
    return data;
}
```

**Java - Caffeine Cache:**
```java
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

Cache<String, Data> cache = Caffeine.newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build();

public Data getData(String key) {
    return cache.get(key, k -> loadFromDatabase(k));
}
```

---

### 2. Database Query Caching

**ORM Query Cache (SQLAlchemy):**
```python
from sqlalchemy.orm import scoped_session, sessionmaker
from cachetools import TTLCache

query_cache = TTLCache(maxsize=1000, ttl=300)  # 5 min TTL

@cache_query(query_cache)
def get_active_users():
    return session.query(User).filter(User.active == True).all()
```

**Query Result Cache (Sequelize):**
```javascript
const { Sequelize } = require('sequelize');
const cache = new Map();

async function getCachedUsers() {
    const cacheKey = 'active_users';

    if (cache.has(cacheKey)) {
        return cache.get(cacheKey);
    }

    const users = await User.findAll({ where: { active: true } });
    cache.set(cacheKey, users);

    setTimeout(() => cache.delete(cacheKey), 300000); // 5 min TTL
    return users;
}
```

---

### 3. HTTP Caching

**Cache-Control Headers:**
```python
# Flask example
from flask import make_response

@app.route('/api/static-data')
def get_static_data():
    response = make_response(jsonify(data))
    response.headers['Cache-Control'] = 'public, max-age=3600'  # 1 hour
    response.headers['ETag'] = generate_etag(data)
    return response
```

**CDN Caching:**
```javascript
// Express with CDN-friendly headers
app.get('/api/products', (req, res) => {
    res.set({
        'Cache-Control': 'public, max-age=3600, s-maxage=7200',
        'Surrogate-Control': 'max-age=86400'  // CDN-specific
    });
    res.json(products);
});
```

---

### 4. Redis Distributed Cache

**Python - Redis Cache:**
```python
import redis
import json
from functools import wraps

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def redis_cache(ttl=300):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{func.__name__}:{args}:{kwargs}"

            # Check cache
            cached = redis_client.get(cache_key)
            if cached:
                return json.loads(cached)

            # Compute and cache
            result = func(*args, **kwargs)
            redis_client.setex(cache_key, ttl, json.dumps(result))
            return result
        return wrapper
    return decorator

@redis_cache(ttl=600)
def get_user_stats(user_id):
    # Expensive aggregation
    return stats
```

**JavaScript - ioredis:**
```javascript
const Redis = require('ioredis');
const redis = new Redis();

async function getCached(key, fetchFn, ttl = 300) {
    const cached = await redis.get(key);

    if (cached) {
        return JSON.parse(cached);
    }

    const data = await fetchFn();
    await redis.setex(key, ttl, JSON.stringify(data));
    return data;
}

// Usage
const userData = await getCached(
    `user:${userId}`,
    () => fetchUserFromDB(userId),
    600  // 10 minutes
);
```

---

## Caching Patterns

### Pattern 1: Cache-Aside (Lazy Loading)

**Description**: Application checks cache first, loads from source on miss.

```python
def get_user(user_id):
    # 1. Check cache
    cached = cache.get(f'user:{user_id}')
    if cached:
        return cached

    # 2. Cache miss - load from database
    user = db.query(User).get(user_id)

    # 3. Store in cache
    cache.set(f'user:{user_id}', user, ttl=600)
    return user
```

**Pros**: Simple, cache only what's needed
**Cons**: Cache miss penalty, possible stampede

---

### Pattern 2: Write-Through Cache

**Description**: Write to cache and database simultaneously.

```python
def update_user(user_id, data):
    # 1. Update database
    db.query(User).filter_by(id=user_id).update(data)
    db.commit()

    # 2. Update cache
    user = db.query(User).get(user_id)
    cache.set(f'user:{user_id}', user, ttl=600)

    return user
```

**Pros**: Cache always consistent
**Cons**: Write latency (two operations)

---

### Pattern 3: Write-Behind (Write-Back) Cache

**Description**: Write to cache immediately, persist to database asynchronously.

```python
import asyncio
from queue import Queue

write_queue = Queue()

def update_user(user_id, data):
    # 1. Update cache immediately
    cache.set(f'user:{user_id}', data, ttl=600)

    # 2. Queue database write
    write_queue.put(('update_user', user_id, data))

    return data

# Async worker
async def flush_writes():
    while True:
        if not write_queue.empty():
            operation, user_id, data = write_queue.get()
            db.query(User).filter_by(id=user_id).update(data)
            db.commit()
        await asyncio.sleep(1)
```

**Pros**: Fast writes, batching possible
**Cons**: Risk of data loss, complexity

---

### Pattern 4: Refresh-Ahead

**Description**: Proactively refresh cache before expiration.

```python
import time

def get_user_with_refresh(user_id):
    cache_key = f'user:{user_id}'
    cached = cache.get_with_metadata(cache_key)

    if cached:
        # If 80% of TTL elapsed, refresh in background
        if cached.age > cached.ttl * 0.8:
            asyncio.create_task(refresh_cache(user_id))
        return cached.data

    # Cache miss
    return load_and_cache_user(user_id)

async def refresh_cache(user_id):
    user = await load_user_from_db(user_id)
    cache.set(f'user:{user_id}', user, ttl=600)
```

**Pros**: Prevents cache misses
**Cons**: May refresh unused data

---

## Cache Invalidation Strategies

### 1. Time-Based Expiration (TTL)

```python
# Simple TTL
cache.set('key', value, ttl=300)  # Expires in 5 minutes

# Sliding expiration
def get_with_sliding_ttl(key, ttl=300):
    value = cache.get(key)
    if value:
        cache.set(key, value, ttl=ttl)  # Reset expiration
    return value
```

---

### 2. Event-Based Invalidation

```python
# Invalidate on update
def update_user(user_id, data):
    db.query(User).filter_by(id=user_id).update(data)
    db.commit()

    # Invalidate related caches
    cache.delete(f'user:{user_id}')
    cache.delete(f'user:{user_id}:posts')
    cache.delete('user:list')
```

---

### 3. Tag-Based Invalidation

```python
class TaggedCache:
    def set(self, key, value, tags=None, ttl=300):
        self.cache.set(key, value, ttl=ttl)

        if tags:
            for tag in tags:
                tag_key = f'tag:{tag}'
                keys = self.cache.get(tag_key) or set()
                keys.add(key)
                self.cache.set(tag_key, keys)

    def invalidate_tag(self, tag):
        tag_key = f'tag:{tag}'
        keys = self.cache.get(tag_key) or set()

        for key in keys:
            self.cache.delete(key)

        self.cache.delete(tag_key)

# Usage
cache.set('user:1', user_data, tags=['user', 'user:1'])
cache.set('user:2', user_data, tags=['user', 'user:2'])

# Invalidate all user caches
cache.invalidate_tag('user')
```

---

## Cache Sizing and Eviction

### LRU (Least Recently Used)

```python
from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key):
        if key not in self.cache:
            return None
        # Move to end (most recently used)
        self.cache.move_to_end(key)
        return self.cache[key]

    def set(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value

        if len(self.cache) > self.capacity:
            # Remove least recently used (first item)
            self.cache.popitem(last=False)
```

---

### LFU (Least Frequently Used)

```python
from collections import defaultdict

class LFUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = {}
        self.freq = defaultdict(int)
        self.min_freq = 0

    def get(self, key):
        if key not in self.cache:
            return None

        # Increment frequency
        self.freq[key] += 1
        return self.cache[key]

    def set(self, key, value):
        if self.capacity == 0:
            return

        if len(self.cache) >= self.capacity:
            # Remove least frequently used
            min_key = min(self.freq, key=self.freq.get)
            del self.cache[min_key]
            del self.freq[min_key]

        self.cache[key] = value
        self.freq[key] = 1
```

---

## Cache Stampede Prevention

**Problem**: Many requests try to refresh expired cache simultaneously.

**Solution - Lock-Based:**
```python
import threading

cache_locks = {}

def get_with_stampede_prevention(key, load_fn, ttl=300):
    # Check cache
    value = cache.get(key)
    if value is not None:
        return value

    # Acquire lock for this key
    if key not in cache_locks:
        cache_locks[key] = threading.Lock()

    lock = cache_locks[key]

    with lock:
        # Double-check cache (might be populated by another thread)
        value = cache.get(key)
        if value is not None:
            return value

        # Load and cache
        value = load_fn()
        cache.set(key, value, ttl=ttl)
        return value
```

**Solution - Probabilistic Early Expiration:**
```python
import random
import time

def get_with_early_expiration(key, load_fn, ttl=300):
    cached = cache.get_with_metadata(key)

    if cached:
        # Probabilistically refresh before expiration
        age = time.time() - cached.created_at
        probability = age / ttl

        if random.random() < probability * 0.1:  # Max 10% chance
            # Refresh in background
            asyncio.create_task(refresh_cache(key, load_fn, ttl))

        return cached.value

    # Cache miss
    value = load_fn()
    cache.set(key, value, ttl=ttl)
    return value
```

---

## Best Practices

1. **Set Appropriate TTLs**: Balance freshness vs. performance
2. **Monitor Cache Hit Rate**: Aim for >80% hit rate
3. **Size Limits**: Prevent unbounded growth
4. **Invalidate on Write**: Keep cache consistent
5. **Cache Warm-Up**: Pre-populate critical data
6. **Compression**: For large cached values
7. **Serialization**: Use efficient formats (MessagePack, Protocol Buffers)
8. **Monitor Memory**: Track cache memory usage
9. **Fallback Strategy**: Handle cache failures gracefully
10. **Document Cache Keys**: Use consistent naming conventions

---

## Cache Metrics to Monitor

- **Hit Rate**: cache hits / (cache hits + misses)
- **Miss Rate**: cache misses / (cache hits + misses)
- **Eviction Rate**: Items evicted per second
- **Memory Usage**: Current cache size
- **Latency**: Cache get/set operation time
- **TTL Distribution**: How long items stay cached

**Target Metrics:**
- Hit rate: > 80%
- Get latency: < 1ms (in-memory), < 10ms (Redis)
- Memory usage: < 80% capacity
