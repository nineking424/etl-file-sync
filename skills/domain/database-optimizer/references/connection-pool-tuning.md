# Connection Pool Tuning

HikariCP configuration and connection management best practices.

## HikariCP Configuration

### Production Configuration
```properties
# Pool sizing
hikari.maximum-pool-size=20
hikari.minimum-idle=5

# Timeouts (milliseconds)
hikari.connection-timeout=30000
hikari.idle-timeout=600000
hikari.max-lifetime=1800000

# Leak detection
hikari.leak-detection-threshold=60000

# Connection test
hikari.connection-test-query=SELECT 1

# Keepalive
hikari.keepalive-time=300000
```

### Pool Size Calculation
```
# Formula: connections = ((core_count * 2) + effective_spindle_count)
# For 4-core CPU with SSD: (4 * 2) + 1 = 9 connections

# Conservative: 10-20 connections
# Aggressive: 20-50 connections
# Monitor: Active connections vs pool size
```

## Oracle-Specific Configuration
```properties
hikari.data-source-class-name=oracle.jdbc.pool.OracleDataSource
hikari.data-source-properties.url=jdbc:oracle:thin:@//host:1521/service
hikari.data-source-properties.user=username
hikari.data-source-properties.password=password

# Oracle optimizations
hikari.data-source-properties.oracle.net.CONNECT_TIMEOUT=5000
hikari.data-source-properties.oracle.jdbc.ReadTimeout=60000
hikari.data-source-properties.oracle.jdbc.timezoneAsRegion=false
```

## Monitoring

### HikariCP Metrics
```java
HikariPoolMXBean poolProxy = pool.getHikariPoolMXBean();

// Monitor these metrics
int activeConnections = poolProxy.getActiveConnections();
int idleConnections = poolProxy.getIdleConnections();
int totalConnections = poolProxy.getTotalConnections();
int threadsAwaitingConnection = poolProxy.getThreadsAwaitingConnection();

// Alert if:
// - threadsAwaitingConnection > 0 (pool exhausted)
// - activeConnections == maximumPoolSize (may need more)
```

## Common Issues

### Connection Leak Detection
```java
// Enable leak detection
config.setLeakDetectionThreshold(60000); // 60 seconds

// Logs will show:
// "Connection leak detection triggered, stack trace follows"
// Review code to ensure connections are closed
```

### Pool Exhaustion
```
Symptoms: java.sql.SQLTimeoutException: Connection is not available
Solutions:
1. Increase maximum-pool-size
2. Reduce connection-timeout
3. Find and fix connection leaks
4. Optimize query performance
```

## Best Practices

1. **Start small** (10-20 connections) and scale up
2. **Monitor metrics** continuously
3. **Enable leak detection** in development
4. **Set appropriate timeouts**
5. **Use try-with-resources** to ensure connection closure
6. **Test under load** before production
7. **Never exceed database max connections**
8. **Configure keepalive** for firewall issues
9. **Use same pool** across application
10. **Regular review** of pool metrics and tuning
