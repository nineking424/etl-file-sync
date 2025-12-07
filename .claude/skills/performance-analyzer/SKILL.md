---
name: performance-analyzer
description: Analyze and optimize code performance with Big-O complexity analysis, bottleneck detection, and actionable optimization recommendations
---

# Performance Analyzer Skill

## Overview

The Performance Analyzer skill helps you identify and resolve performance bottlenecks in your code. It provides Big-O complexity analysis, detects common performance anti-patterns, and offers concrete optimization recommendations with measurable impact.

## Core Capabilities

### 1. Big-O Complexity Analysis
- Analyze time complexity of algorithms
- Identify space complexity issues
- Detect quadratic and exponential complexity patterns
- Suggest optimal algorithmic improvements
- Calculate expected performance at scale

### 2. N+1 Query Detection
- Identify N+1 query problems in ORMs
- Detect missing eager loading
- Find inefficient database access patterns
- Suggest batch loading strategies
- Recommend query optimization techniques

### 3. Memory Leak Pattern Identification
- Detect unclosed resources
- Identify circular references
- Find event listener leaks
- Spot cache growth issues
- Detect closure memory leaks

### 4. Caching Optimization
- Identify cacheable computations
- Suggest caching strategies (memoization, HTTP caching, query caching)
- Detect cache invalidation issues
- Recommend cache sizing
- Find cache stampede vulnerabilities

### 5. Batch Processing Recommendations
- Identify operations suitable for batching
- Suggest batch size optimization
- Recommend parallel processing opportunities
- Detect serial processing bottlenecks
- Propose async/await optimizations

## Workflow

### 1. Performance Profiling
- Identify performance-critical code paths
- Measure current execution time
- Establish performance baseline
- Identify resource consumption (CPU, memory, I/O)

### 2. Bottleneck Detection
- Analyze algorithmic complexity
- Profile database queries
- Check memory allocation patterns
- Identify I/O blocking operations
- Detect synchronous operations in async contexts

### 3. Root Cause Analysis
- Determine primary performance bottleneck
- Quantify impact of each issue
- Prioritize optimization opportunities
- Estimate improvement potential

### 4. Optimization Recommendations
- Suggest specific code changes
- Provide before/after examples
- Estimate performance improvement
- Consider trade-offs (complexity vs. performance)
- Recommend profiling tools

### 5. Verification
- Measure post-optimization performance
- Compare against baseline
- Validate improvement meets requirements
- Check for regression in other areas

## Performance Analysis Checklist

### Algorithm Complexity
- [ ] Time complexity is optimal for use case
- [ ] No nested loops on large datasets (O(n²))
- [ ] No exponential algorithms (O(2ⁿ))
- [ ] Appropriate data structures used
- [ ] Sorting algorithms are efficient
- [ ] Search operations use indexes

### Database Performance
- [ ] No N+1 query problems
- [ ] Proper indexing on query columns
- [ ] Eager loading for associations
- [ ] Batch operations instead of loops
- [ ] Query result limiting/pagination
- [ ] Connection pooling configured

### Memory Management
- [ ] No memory leaks
- [ ] Resources properly closed/disposed
- [ ] Event listeners cleaned up
- [ ] Cache has size limits
- [ ] Large objects properly dereferenced
- [ ] Streaming for large data

### I/O Operations
- [ ] Async I/O for non-blocking operations
- [ ] Batch I/O operations
- [ ] Buffering for file operations
- [ ] Compression for network transfers
- [ ] Connection reuse (keep-alive)
- [ ] Parallel I/O where appropriate

### Caching Strategy
- [ ] Expensive computations cached
- [ ] Cache invalidation strategy defined
- [ ] HTTP caching headers set
- [ ] Database query caching enabled
- [ ] Cache hit rate monitored
- [ ] Cache stampede prevention

### Rendering/UI Performance
- [ ] Virtual scrolling for large lists
- [ ] Debouncing/throttling user input
- [ ] Lazy loading of images/components
- [ ] Code splitting for bundles
- [ ] Memoization of expensive renders
- [ ] RequestAnimationFrame for animations

## Usage Examples

### Example 1: N+1 Query Problem

**Inefficient Code (Python/SQLAlchemy):**
```python
# N+1 Problem: 1 query for users + N queries for posts
def get_users_with_posts():
    users = User.query.all()  # 1 query
    result = []
    for user in users:  # N iterations
        posts = user.posts  # N queries (lazy loading)
        result.append({
            'user': user.name,
            'post_count': len(posts)
        })
    return result

# Performance: O(N) queries where N = number of users
# If 100 users: 101 queries!
```

**Performance Issue:**
- **Problem**: N+1 query anti-pattern
- **Impact**: 101 queries for 100 users (1 + 100)
- **Big-O**: O(N) database queries
- **Latency**: ~1s for 100 users (assuming 10ms per query)

**Optimized Code:**
```python
# Eager loading: 1 query with JOIN
def get_users_with_posts():
    users = User.query.options(
        joinedload(User.posts)
    ).all()  # 1 query with JOIN

    result = []
    for user in users:
        result.append({
            'user': user.name,
            'post_count': len(user.posts)  # No additional query
        })
    return result

# Performance: 1 query regardless of N
# If 100 users: 1 query
```

**Performance Improvement:**
- **Queries reduced**: 101 → 1 (99% reduction)
- **Expected speedup**: ~10-100x
- **Scales**: O(1) queries regardless of user count

### Example 2: Quadratic Time Complexity

**Inefficient Code (JavaScript):**
```javascript
// O(n²) complexity: Nested loops
function findDuplicates(arr) {
    const duplicates = [];
    for (let i = 0; i < arr.length; i++) {
        for (let j = i + 1; j < arr.length; j++) {
            if (arr[i] === arr[j] && !duplicates.includes(arr[i])) {
                duplicates.push(arr[i]);
            }
        }
    }
    return duplicates;
}

// Performance: O(n²) time
// 1,000 items: ~1,000,000 comparisons
// 10,000 items: ~100,000,000 comparisons
```

**Performance Issue:**
- **Complexity**: O(n²) - quadratic time
- **Problem**: Nested loops on array
- **Impact**: Exponential growth in execution time
- **Example**: 1,000 items = 1M operations, 10,000 items = 100M operations

**Optimized Code:**
```javascript
// O(n) complexity: Hash set
function findDuplicates(arr) {
    const seen = new Set();
    const duplicates = new Set();

    for (const item of arr) {
        if (seen.has(item)) {
            duplicates.add(item);
        } else {
            seen.add(item);
        }
    }

    return Array.from(duplicates);
}

// Performance: O(n) time, O(n) space
// 1,000 items: ~1,000 operations
// 10,000 items: ~10,000 operations
```

**Performance Improvement:**
- **Complexity**: O(n²) → O(n)
- **1,000 items**: 1,000,000 ops → 1,000 ops (1000x faster)
- **10,000 items**: 100,000,000 ops → 10,000 ops (10,000x faster)
- **Trade-off**: O(n) extra space for hash set

### Example 3: Memory Leak - Event Listeners

**Inefficient Code (JavaScript):**
```javascript
class Component {
    constructor() {
        this.data = new Array(1000000); // Large data
        this.handleResize = () => {
            this.render();
        };

        // Event listener added but never removed
        window.addEventListener('resize', this.handleResize);
    }

    destroy() {
        // Memory leak: event listener not removed
        // Component can't be garbage collected
    }
}

// Problem: Each new Component instance adds listener but never removes
// 100 Component instances = 100 listeners + 100MB of leaked memory
```

**Performance Issue:**
- **Problem**: Event listener memory leak
- **Impact**: Component never garbage collected
- **Memory**: ~1MB per instance (grows unbounded)
- **Side effect**: Multiple handlers firing on resize

**Optimized Code:**
```javascript
class Component {
    constructor() {
        this.data = new Array(1000000);
        this.handleResize = () => {
            this.render();
        };
        window.addEventListener('resize', this.handleResize);
    }

    destroy() {
        // Clean up: Remove event listener
        window.removeEventListener('resize', this.handleResize);

        // Dereference large data
        this.data = null;
    }
}

// Proper cleanup allows garbage collection
```

**Performance Improvement:**
- **Memory leak eliminated**: Component properly garbage collected
- **Memory saved**: ~1MB per destroyed component
- **Event overhead**: No duplicate handlers

### Example 4: Inefficient Rendering

**Inefficient Code (React):**
```javascript
function UserList({ users }) {
    // Re-renders all items on every update
    return (
        <div>
            {users.map(user => (
                <div key={user.id}>
                    <Avatar src={user.avatar} />
                    <ExpensiveComponent data={user} />
                </div>
            ))}
        </div>
    );
}

function ExpensiveComponent({ data }) {
    // Expensive calculation runs on every render
    const result = performExpensiveCalculation(data);
    return <div>{result}</div>;
}

// Problem: All items re-render when parent updates
// 1,000 users = 1,000 expensive calculations per update
```

**Performance Issue:**
- **Problem**: Unnecessary re-renders
- **Impact**: Expensive calculations repeated
- **Time**: ~1ms per calculation × 1,000 users = 1 second per update

**Optimized Code:**
```javascript
import { memo, useMemo } from 'react';

// Memoize component: only re-render if props change
const ExpensiveComponent = memo(({ data }) => {
    // Memoize calculation: only recalculate if data changes
    const result = useMemo(
        () => performExpensiveCalculation(data),
        [data]
    );
    return <div>{result}</div>;
});

function UserList({ users }) {
    return (
        <div>
            {users.map(user => (
                <div key={user.id}>
                    <Avatar src={user.avatar} />
                    <ExpensiveComponent data={user} />
                </div>
            ))}
        </div>
    );
}

// Virtual scrolling for large lists
import { FixedSizeList } from 'react-window';

function VirtualizedUserList({ users }) {
    const Row = ({ index, style }) => (
        <div style={style}>
            <Avatar src={users[index].avatar} />
            <ExpensiveComponent data={users[index]} />
        </div>
    );

    return (
        <FixedSizeList
            height={600}
            itemCount={users.length}
            itemSize={50}
            width="100%"
        >
            {Row}
        </FixedSizeList>
    );
}
```

**Performance Improvement:**
- **Memoization**: Only changed items recalculate
- **Virtual scrolling**: Only render visible items (~20 items vs 1,000)
- **Expected speedup**: 50-100x for large lists

### Example 5: Batch Processing

**Inefficient Code (Python):**
```python
# Serial processing: One at a time
def process_images(image_urls):
    results = []
    for url in image_urls:  # Sequential
        image = download_image(url)  # 100ms per image
        resized = resize_image(image)  # 50ms per image
        results.append(upload_image(resized))  # 100ms per image
    return results

# Performance: 100 images × 250ms = 25 seconds
```

**Performance Issue:**
- **Problem**: Serial processing of independent tasks
- **Impact**: Total time = sum of all task times
- **Example**: 100 images × 250ms = 25 seconds

**Optimized Code:**
```python
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# Async I/O: Parallel downloads/uploads
async def process_images_async(image_urls):
    async with aiohttp.ClientSession() as session:
        tasks = [process_single_image(url, session) for url in image_urls]
        results = await asyncio.gather(*tasks)
    return results

async def process_single_image(url, session):
    # Download (I/O-bound: use async)
    async with session.get(url) as response:
        image_data = await response.read()

    # Resize (CPU-bound: use process pool)
    loop = asyncio.get_event_loop()
    with ProcessPoolExecutor() as pool:
        resized = await loop.run_in_executor(pool, resize_image, image_data)

    # Upload (I/O-bound: use async)
    async with session.post(upload_url, data=resized) as response:
        return await response.json()

# Performance: 100 images in parallel ~ 250ms (100x faster)
```

**Performance Improvement:**
- **Parallelization**: 100 concurrent operations
- **Time**: 25 seconds → 0.25 seconds (100x faster)
- **Trade-off**: Higher memory usage, more complex code

## Integration Points

### With Code Reviewer
- Performance Analyzer focuses on runtime efficiency
- Code Reviewer handles code quality and maintainability
- Both identify inefficient algorithms from different perspectives

### With Tester
- Performance tests should validate optimizations
- Load testing for scalability verification
- Benchmark tests for regression detection

### With Debugger
- Debugger helps identify why code is slow
- Performance Analyzer suggests what to optimize
- Both use profiling data

## Performance Profiling Tools

### Python
- **cProfile**: Built-in profiler
- **line_profiler**: Line-by-line profiling
- **memory_profiler**: Memory usage profiling
- **py-spy**: Sampling profiler (production-safe)

### JavaScript/Node.js
- **Chrome DevTools**: Performance tab
- **Node.js profiler**: Built-in V8 profiler
- **clinic.js**: Performance diagnostics
- **autocannon**: HTTP load testing

### Java
- **JProfiler**: Comprehensive profiler
- **VisualVM**: JVM monitoring
- **YourKit**: Performance profiler
- **Java Flight Recorder**: Production profiler

## Best Practices

1. **Measure First**: Always profile before optimizing
2. **Focus on Bottlenecks**: Optimize the slowest parts (80/20 rule)
3. **Set Targets**: Define acceptable performance metrics
4. **Test at Scale**: Profile with realistic data volumes
5. **Consider Trade-offs**: Performance vs. maintainability
6. **Monitor Production**: Use APM tools to detect regressions
7. **Optimize Algorithms First**: Better algorithm > micro-optimizations
8. **Cache Wisely**: Cache expensive operations, not everything
9. **Async for I/O**: Use async/await for I/O-bound operations
10. **Batch Operations**: Reduce round trips to databases/APIs

## Performance Metrics

### Response Time Targets
- **Interactive UI**: < 100ms for user actions
- **API Response**: < 200ms for simple queries
- **Page Load**: < 2 seconds for initial load
- **Database Query**: < 50ms for indexed queries

### Throughput Targets
- **API**: > 1000 requests/second
- **Database**: > 10,000 queries/second
- **Message Queue**: > 100,000 messages/second

### Resource Utilization
- **CPU**: < 70% average utilization
- **Memory**: < 80% of available memory
- **Disk I/O**: < 80% capacity
- **Network**: < 70% bandwidth

## References

See the `references/` directory for detailed guides on:
- **performance-patterns.md**: Common performance optimization patterns
- **anti-patterns.md**: Performance anti-patterns to avoid
- **caching-strategies.md**: Caching techniques and strategies
- **optimization-techniques.md**: Advanced optimization techniques
