# Logging Conventions

## Log Levels

### Level Guidelines

| Level | Purpose | When to Use | Example |
|-------|---------|-------------|---------|
| **DEBUG** | Detailed diagnostic information | Development and troubleshooting | Method entry/exit, variable values, detailed flow |
| **INFO** | General informational messages | Important business events | User created, order placed, service started |
| **WARN** | Potentially harmful situations | Recoverable errors, deprecated usage | Retry attempt, fallback used, missing optional config |
| **ERROR** | Error events that might still allow the app to continue | Exceptions, failures | Database connection failed, API call failed |

### Level Selection Examples

**Java:**
```java
@Service
@Slf4j
public class OrderService {

    public Order createOrder(OrderRequest request) {
        // DEBUG: Detailed diagnostic information
        log.debug("Creating order with request: {}", request);

        // INFO: Important business event
        log.info("Order created for user: {}, total: {}", request.getUserId(), request.getTotal());

        // WARN: Potentially harmful but recoverable
        if (inventoryService.getStock(request.getProductId()) < 10) {
            log.warn("Low stock for product: {}, current stock: {}",
                request.getProductId(), inventoryService.getStock(request.getProductId()));
        }

        // ERROR: Actual error occurred
        try {
            return orderRepository.save(order);
        } catch (DataAccessException e) {
            log.error("Failed to save order for user: {}", request.getUserId(), e);
            throw new ServiceException("Order creation failed", e);
        }
    }
}
```

## Structured Logging

### Parameterized Logging

**Java (SLF4J):**
```java
@Slf4j
public class UserService {

    // GOOD: Parameterized logging
    public void processUser(User user) {
        log.info("Processing user: {}, email: {}, status: {}",
            user.getId(), user.getEmail(), user.getStatus());
    }

    // BAD: String concatenation
    public void processUserBad(User user) {
        log.info("Processing user: " + user.getId() +
            ", email: " + user.getEmail() +
            ", status: " + user.getStatus());  // Avoid this!
    }

    // GOOD: Logging with exception
    public void handleError(Long userId, Exception e) {
        log.error("Error processing user: {}", userId, e);
    }
}
```

**Python:**
```python
import logging

logger = logging.getLogger(__name__)

class UserService:
    # GOOD: Parameterized logging
    def process_user(self, user: User):
        logger.info(
            "Processing user: %s, email: %s, status: %s",
            user.id, user.email, user.status
        )

    # GOOD: f-string (Python 3.6+)
    def process_user_v2(self, user: User):
        logger.info(f"Processing user: {user.id}, email: {user.email}, status: {user.status}")

    # GOOD: Logging with exception
    def handle_error(self, user_id: int, e: Exception):
        logger.error(f"Error processing user: {user_id}", exc_info=True)
```

**Kotlin:**
```kotlin
class UserService(
    private val logger: Logger = LoggerFactory.getLogger(UserService::class.java)
) {
    // GOOD: Parameterized logging
    fun processUser(user: User) {
        logger.info("Processing user: {}, email: {}, status: {}",
            user.id, user.email, user.status)
    }

    // GOOD: Kotlin logging with lambda (kotlin-logging)
    fun processUserV2(user: User) {
        logger.info { "Processing user: ${user.id}, email: ${user.email}, status: ${user.status}" }
    }

    // GOOD: Logging with exception
    fun handleError(userId: Long, e: Exception) {
        logger.error("Error processing user: $userId", e)
    }
}
```

### Contextual Information

**Include relevant context in logs:**
```java
@Slf4j
public class PaymentService {

    public Payment processPayment(PaymentRequest request) {
        // Include all relevant context
        log.info("Processing payment - userId: {}, amount: {}, currency: {}, method: {}",
            request.getUserId(),
            request.getAmount(),
            request.getCurrency(),
            request.getPaymentMethod());

        try {
            Payment payment = paymentGateway.process(request);

            log.info("Payment successful - paymentId: {}, transactionId: {}, userId: {}",
                payment.getId(),
                payment.getTransactionId(),
                request.getUserId());

            return payment;
        } catch (PaymentException e) {
            log.error("Payment failed - userId: {}, amount: {}, reason: {}",
                request.getUserId(),
                request.getAmount(),
                e.getMessage(),
                e);
            throw e;
        }
    }
}
```

## MDC (Mapped Diagnostic Context)

### Request Tracking

**Java (SLF4J MDC):**
```java
@Component
public class RequestLoggingFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(
        HttpServletRequest request,
        HttpServletResponse response,
        FilterChain filterChain
    ) throws ServletException, IOException {
        try {
            // Add request ID to MDC
            String requestId = UUID.randomUUID().toString();
            MDC.put("requestId", requestId);
            MDC.put("userId", getCurrentUserId(request));
            MDC.put("clientIp", request.getRemoteAddr());

            filterChain.doFilter(request, response);
        } finally {
            // Always clear MDC
            MDC.clear();
        }
    }
}

@Service
@Slf4j
public class UserService {
    public User createUser(UserRequest request) {
        // requestId, userId, and clientIp automatically included in logs
        log.info("Creating user: {}", request.getEmail());
        // Log output: [requestId=abc123] [userId=456] [clientIp=192.168.1.1] Creating user: john@example.com
        return userRepository.save(user);
    }
}
```

**Python (contextvars):**
```python
import logging
import uuid
from contextvars import ContextVar

request_id: ContextVar[str] = ContextVar('request_id', default='')
user_id: ContextVar[str] = ContextVar('user_id', default='')

class ContextFilter(logging.Filter):
    def filter(self, record):
        record.request_id = request_id.get()
        record.user_id = user_id.get()
        return True

# Configure logging
logging.basicConfig(
    format='[%(request_id)s] [%(user_id)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.addFilter(ContextFilter())

# Middleware
class RequestLoggingMiddleware:
    def __call__(self, request):
        request_id.set(str(uuid.uuid4()))
        user_id.set(get_current_user_id(request))
        # Process request
        logger.info(f"Processing request: {request.path}")
```

## Performance Considerations

### Avoid Expensive Operations

```java
@Slf4j
public class DataService {

    // BAD: Always executes expensive operation
    public void processData(Data data) {
        log.debug("Processing data: " + data.toExpensiveString());  // Always called!
    }

    // GOOD: Use parameterized logging
    public void processDataGood(Data data) {
        log.debug("Processing data: {}", data);  // toString() only called if DEBUG enabled
    }

    // GOOD: Guard with level check for very expensive operations
    public void processDataBest(Data data) {
        if (log.isDebugEnabled()) {
            log.debug("Processing data: {}", data.toExpensiveString());
        }
    }
}
```

## Security Considerations

### Never Log Sensitive Information

```java
@Slf4j
public class UserService {

    // BAD: Logging sensitive information
    public void loginBad(LoginRequest request) {
        log.info("Login attempt - email: {}, password: {}",
            request.getEmail(),
            request.getPassword());  // NEVER LOG PASSWORDS!
    }

    // GOOD: Mask or omit sensitive data
    public void loginGood(LoginRequest request) {
        log.info("Login attempt - email: {}", request.getEmail());
    }

    // GOOD: Sanitize user objects before logging
    public void updateUser(User user) {
        log.info("Updating user: {}", sanitizeUser(user));
    }

    private User sanitizeUser(User user) {
        User sanitized = new User();
        sanitized.setId(user.getId());
        sanitized.setEmail(maskEmail(user.getEmail()));
        // Don't include password, SSN, credit card, etc.
        return sanitized;
    }

    private String maskEmail(String email) {
        if (email == null) return null;
        int atIndex = email.indexOf('@');
        if (atIndex <= 1) return email;
        return email.charAt(0) + "***" + email.substring(atIndex);
    }
}
```

**Sensitive data to never log:**
- Passwords
- API keys, tokens, secrets
- Credit card numbers
- Social Security Numbers (SSN)
- Personal Identifiable Information (PII) when not necessary
- Session IDs

## Logging Patterns by Operation Type

### CRUD Operations

```java
@Slf4j
@Service
public class UserService {

    public User create(UserRequest request) {
        log.debug("Creating user with email: {}", request.getEmail());

        User user = userRepository.save(buildUser(request));

        log.info("User created successfully - id: {}, email: {}",
            user.getId(), user.getEmail());

        return user;
    }

    public Optional<User> findById(Long id) {
        log.debug("Finding user by id: {}", id);

        Optional<User> user = userRepository.findById(id);

        if (user.isPresent()) {
            log.debug("User found: {}", id);
        } else {
            log.debug("User not found: {}", id);
        }

        return user;
    }

    public User update(Long id, UserRequest request) {
        log.debug("Updating user: {}", id);

        User user = userRepository.findById(id)
            .orElseThrow(() -> {
                log.warn("Update failed - user not found: {}", id);
                return new ResourceNotFoundException("User", id);
            });

        updateUserFields(user, request);
        User updated = userRepository.save(user);

        log.info("User updated successfully - id: {}", id);

        return updated;
    }

    public void delete(Long id) {
        log.debug("Deleting user: {}", id);

        if (!userRepository.existsById(id)) {
            log.warn("Delete failed - user not found: {}", id);
            throw new ResourceNotFoundException("User", id);
        }

        userRepository.deleteById(id);

        log.info("User deleted successfully - id: {}", id);
    }
}
```

### External API Calls

```java
@Slf4j
@Service
public class PaymentGatewayService {

    public PaymentResponse processPayment(PaymentRequest request) {
        log.info("Calling payment gateway - amount: {}, currency: {}, method: {}",
            request.getAmount(), request.getCurrency(), request.getMethod());

        long startTime = System.currentTimeMillis();

        try {
            PaymentResponse response = paymentGatewayClient.process(request);

            long duration = System.currentTimeMillis() - startTime;

            log.info("Payment gateway success - transactionId: {}, duration: {}ms",
                response.getTransactionId(), duration);

            return response;

        } catch (PaymentGatewayException e) {
            long duration = System.currentTimeMillis() - startTime;

            log.error("Payment gateway failed - amount: {}, duration: {}ms, error: {}",
                request.getAmount(), duration, e.getMessage(), e);

            throw new ServiceException("Payment processing failed", e);
        }
    }
}
```

### Batch Processing

```java
@Slf4j
@Service
public class DataImportService {

    public ImportResult importData(List<DataRecord> records) {
        log.info("Starting data import - total records: {}", records.size());

        int successCount = 0;
        int failureCount = 0;
        List<String> errors = new ArrayList<>();

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);

            try {
                processRecord(record);
                successCount++;

                // Log progress every 100 records
                if ((i + 1) % 100 == 0) {
                    log.info("Import progress - processed: {}/{}, success: {}, failures: {}",
                        i + 1, records.size(), successCount, failureCount);
                }

            } catch (Exception e) {
                failureCount++;
                String error = String.format("Record %d failed: %s", i, e.getMessage());
                errors.add(error);
                log.warn("Failed to process record {}: {}", i, e.getMessage());
            }
        }

        log.info("Data import completed - total: {}, success: {}, failures: {}",
            records.size(), successCount, failureCount);

        return new ImportResult(successCount, failureCount, errors);
    }
}
```

### Async Operations

```java
@Slf4j
@Service
public class AsyncEmailService {

    @Async
    public CompletableFuture<Void> sendEmail(EmailRequest request) {
        log.info("Async email task started - recipient: {}, subject: {}",
            request.getRecipient(), request.getSubject());

        try {
            emailClient.send(request);

            log.info("Async email sent successfully - recipient: {}",
                request.getRecipient());

            return CompletableFuture.completedFuture(null);

        } catch (Exception e) {
            log.error("Async email failed - recipient: {}, subject: {}",
                request.getRecipient(), request.getSubject(), e);

            return CompletableFuture.failedFuture(e);
        }
    }
}
```

## Log Configuration

### Logback Configuration (Java/Kotlin)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <!-- Console appender -->
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] [%X{requestId}] [%X{userId}] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- File appender -->
    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/application.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>logs/application-%d{yyyy-MM-dd}.%i.log</fileNamePattern>
            <maxHistory>30</maxHistory>
            <totalSizeCap>1GB</totalSizeCap>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] [%X{requestId}] [%X{userId}] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- JSON appender for structured logging -->
    <appender name="JSON" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/application.json</file>
        <encoder class="net.logstash.logback.encoder.LogstashEncoder"/>
    </appender>

    <!-- Root logger -->
    <root level="INFO">
        <appender-ref ref="CONSOLE" />
        <appender-ref ref="FILE" />
    </root>

    <!-- Package-specific log levels -->
    <logger name="com.company.application" level="DEBUG" />
    <logger name="org.springframework" level="INFO" />
    <logger name="org.hibernate" level="WARN" />
</configuration>
```

### Python Logging Configuration

```python
import logging.config

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] [%(request_id)s] %(name)s: %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] [%(request_id)s] [%(name)s:%(lineno)d] %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': 'logs/application.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 10
        }
    },
    'loggers': {
        'myapp': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
            'propagate': False
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file']
    }
}

logging.config.dictConfig(LOGGING_CONFIG)
```

## Best Practices Summary

1. **Use appropriate log levels** - DEBUG for diagnostics, INFO for business events, WARN for potential issues, ERROR for failures
2. **Use parameterized logging** - Improves performance and readability
3. **Include context** - Add relevant information (IDs, timestamps, user info)
4. **Use MDC/context variables** - For request tracking across the application
5. **Never log sensitive data** - Passwords, tokens, PII should never appear in logs
6. **Log method entry/exit at DEBUG level** - For critical operations only
7. **Log all exceptions** - With stack traces at ERROR level
8. **Use structured logging** - JSON format for better parsing and analysis
9. **Configure log rotation** - Prevent logs from filling disk space
10. **Monitor and alert** - Set up monitoring for ERROR level logs
