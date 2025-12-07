# Error Handling Strategies

## Exception Hierarchy

### Custom Exception Design

**Java Implementation:**
```java
// Base application exception
public class ApplicationException extends RuntimeException {
    private final String errorCode;
    private final Map<String, Object> context;

    public ApplicationException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
        this.context = new HashMap<>();
    }

    public ApplicationException(String message, String errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.context = new HashMap<>();
    }

    public ApplicationException addContext(String key, Object value) {
        this.context.put(key, value);
        return this;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public Map<String, Object> getContext() {
        return Collections.unmodifiableMap(context);
    }
}

// Specific exceptions
public class ValidationException extends ApplicationException {
    public ValidationException(String message) {
        super(message, "VALIDATION_ERROR");
    }
}

public class ResourceNotFoundException extends ApplicationException {
    public ResourceNotFoundException(String resource, Object id) {
        super(String.format("%s not found with id: %s", resource, id), "RESOURCE_NOT_FOUND");
        addContext("resource", resource);
        addContext("id", id);
    }
}

public class ServiceException extends ApplicationException {
    public ServiceException(String message, Throwable cause) {
        super(message, "SERVICE_ERROR", cause);
    }
}

public class DatabaseException extends ApplicationException {
    public DatabaseException(String message, Throwable cause) {
        super(message, "DATABASE_ERROR", cause);
    }
}
```

**Python Implementation:**
```python
from typing import Dict, Any, Optional

class ApplicationException(Exception):
    def __init__(
        self,
        message: str,
        error_code: str,
        cause: Optional[Exception] = None
    ):
        super().__init__(message)
        self.error_code = error_code
        self.cause = cause
        self.context: Dict[str, Any] = {}

    def add_context(self, key: str, value: Any) -> 'ApplicationException':
        self.context[key] = value
        return self

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{base_msg} [{self.error_code}] ({context_str})"
        return f"{base_msg} [{self.error_code}]"

class ValidationException(ApplicationException):
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")

class ResourceNotFoundException(ApplicationException):
    def __init__(self, resource: str, resource_id: Any):
        super().__init__(
            f"{resource} not found with id: {resource_id}",
            "RESOURCE_NOT_FOUND"
        )
        self.add_context("resource", resource)
        self.add_context("id", resource_id)

class ServiceException(ApplicationException):
    def __init__(self, message: str, cause: Optional[Exception] = None):
        super().__init__(message, "SERVICE_ERROR", cause)

class DatabaseException(ApplicationException):
    def __init__(self, message: str, cause: Optional[Exception] = None):
        super().__init__(message, "DATABASE_ERROR", cause)
```

**Kotlin Implementation:**
```kotlin
open class ApplicationException(
    message: String,
    val errorCode: String,
    cause: Throwable? = null
) : RuntimeException(message, cause) {
    private val context: MutableMap<String, Any> = mutableMapOf()

    fun addContext(key: String, value: Any): ApplicationException {
        context[key] = value
        return this
    }

    fun getContext(): Map<String, Any> = context.toMap()
}

class ValidationException(message: String) :
    ApplicationException(message, "VALIDATION_ERROR")

class ResourceNotFoundException(resource: String, id: Any) :
    ApplicationException("$resource not found with id: $id", "RESOURCE_NOT_FOUND") {
    init {
        addContext("resource", resource)
        addContext("id", id)
    }
}

class ServiceException(message: String, cause: Throwable? = null) :
    ApplicationException(message, "SERVICE_ERROR", cause)

class DatabaseException(message: String, cause: Throwable? = null) :
    ApplicationException(message, "DATABASE_ERROR", cause)
```

## Try-Catch Patterns

### Specific Exception Handling

**Java:**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {
    private final UserRepository userRepository;

    public User findUserById(Long id) {
        try {
            log.debug("Finding user by id: {}", id);

            if (id == null || id <= 0) {
                throw new ValidationException("User ID must be positive");
            }

            return userRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("User", id));

        } catch (ValidationException e) {
            log.warn("Validation error: {}", e.getMessage());
            throw e;
        } catch (DataAccessException e) {
            log.error("Database error while finding user: {}", id, e);
            throw new DatabaseException("Failed to retrieve user", e)
                .addContext("userId", id);
        } catch (Exception e) {
            log.error("Unexpected error while finding user: {}", id, e);
            throw new ServiceException("Unexpected error occurred", e)
                .addContext("userId", id);
        }
    }
}
```

**Python:**
```python
class UserService:
    def __init__(self, user_repository: UserRepository):
        self._user_repository = user_repository
        self._logger = logging.getLogger(__name__)

    def find_user_by_id(self, user_id: int) -> User:
        try:
            self._logger.debug(f"Finding user by id: {user_id}")

            if not user_id or user_id <= 0:
                raise ValidationException("User ID must be positive")

            user = self._user_repository.find_by_id(user_id)
            if not user:
                raise ResourceNotFoundException("User", user_id)

            return user

        except ValidationException:
            self._logger.warning(f"Validation error for user id: {user_id}")
            raise
        except DatabaseError as e:
            self._logger.error(f"Database error while finding user: {user_id}", exc_info=True)
            raise DatabaseException("Failed to retrieve user", e).add_context("userId", user_id)
        except Exception as e:
            self._logger.error(f"Unexpected error while finding user: {user_id}", exc_info=True)
            raise ServiceException("Unexpected error occurred", e).add_context("userId", user_id)
```

### Resource Management

**Java (try-with-resources):**
```java
public String readFile(String filePath) {
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
        return reader.lines()
            .collect(Collectors.joining("\n"));
    } catch (FileNotFoundException e) {
        log.error("File not found: {}", filePath, e);
        throw new ResourceNotFoundException("File", filePath);
    } catch (IOException e) {
        log.error("Error reading file: {}", filePath, e);
        throw new ServiceException("Failed to read file", e);
    }
}
```

**Python (context managers):**
```python
def read_file(self, file_path: str) -> str:
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except FileNotFoundError as e:
        self._logger.error(f"File not found: {file_path}", exc_info=True)
        raise ResourceNotFoundException("File", file_path)
    except IOError as e:
        self._logger.error(f"Error reading file: {file_path}", exc_info=True)
        raise ServiceException("Failed to read file", e)
```

**Kotlin (use function):**
```kotlin
fun readFile(filePath: String): String {
    return try {
        File(filePath).bufferedReader().use { it.readText() }
    } catch (e: FileNotFoundException) {
        logger.error("File not found: $filePath", e)
        throw ResourceNotFoundException("File", filePath)
    } catch (e: IOException) {
        logger.error("Error reading file: $filePath", e)
        throw ServiceException("Failed to read file", e)
    }
}
```

## Validation Patterns

### Input Validation

**Java (Bean Validation):**
```java
@Data
public class CreateUserRequest {
    @NotNull(message = "Name is required")
    @Size(min = 2, max = 100, message = "Name must be between 2 and 100 characters")
    private String name;

    @NotNull(message = "Email is required")
    @Email(message = "Email must be valid")
    private String email;

    @Pattern(regexp = "^\\+?[1-9]\\d{1,14}$", message = "Phone number must be valid")
    private String phone;
}

@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {
    private final Validator validator;
    private final UserRepository userRepository;

    public User createUser(CreateUserRequest request) {
        // Validate input
        Set<ConstraintViolation<CreateUserRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            String errors = violations.stream()
                .map(ConstraintViolation::getMessage)
                .collect(Collectors.joining(", "));
            throw new ValidationException(errors);
        }

        // Additional business validation
        if (userRepository.findByEmail(request.getEmail()).isPresent()) {
            throw new ValidationException("Email already exists");
        }

        // Create user
        User user = new User();
        user.setName(request.getName());
        user.setEmail(request.getEmail());
        user.setPhone(request.getPhone());

        return userRepository.save(user);
    }
}
```

**Python (Pydantic):**
```python
from pydantic import BaseModel, EmailStr, Field, validator

class CreateUserRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    email: EmailStr
    phone: Optional[str] = Field(None, regex=r'^\+?[1-9]\d{1,14}$')

    @validator('name')
    def name_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError('Name cannot be empty')
        return v

class UserService:
    def __init__(self, user_repository: UserRepository):
        self._user_repository = user_repository
        self._logger = logging.getLogger(__name__)

    def create_user(self, request: CreateUserRequest) -> User:
        # Pydantic automatically validates on instantiation

        # Additional business validation
        existing_user = self._user_repository.find_by_email(request.email)
        if existing_user:
            raise ValidationException("Email already exists")

        # Create user
        user = User(
            name=request.name,
            email=request.email,
            phone=request.phone
        )

        return self._user_repository.save(user)
```

### Manual Validation

**Java:**
```java
private void validateUser(User user) {
    List<String> errors = new ArrayList<>();

    if (user.getName() == null || user.getName().isBlank()) {
        errors.add("Name is required");
    }

    if (user.getEmail() == null || !user.getEmail().matches("^[A-Za-z0-9+_.-]+@(.+)$")) {
        errors.add("Valid email is required");
    }

    if (user.getAge() != null && user.getAge() < 18) {
        errors.add("User must be at least 18 years old");
    }

    if (!errors.isEmpty()) {
        throw new ValidationException(String.join(", ", errors));
    }
}
```

## Error Response Handling

### REST API Error Response

**Java (Spring Boot):**
```java
@Data
@Builder
public class ErrorResponse {
    private String errorCode;
    private String message;
    private LocalDateTime timestamp;
    private Map<String, Object> context;
}

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(ValidationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleValidationException(ValidationException ex) {
        log.warn("Validation error: {}", ex.getMessage());
        return ErrorResponse.builder()
            .errorCode(ex.getErrorCode())
            .message(ex.getMessage())
            .timestamp(LocalDateTime.now())
            .context(ex.getContext())
            .build();
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleResourceNotFoundException(ResourceNotFoundException ex) {
        log.warn("Resource not found: {}", ex.getMessage());
        return ErrorResponse.builder()
            .errorCode(ex.getErrorCode())
            .message(ex.getMessage())
            .timestamp(LocalDateTime.now())
            .context(ex.getContext())
            .build();
    }

    @ExceptionHandler(ServiceException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleServiceException(ServiceException ex) {
        log.error("Service error: {}", ex.getMessage(), ex);
        return ErrorResponse.builder()
            .errorCode(ex.getErrorCode())
            .message("An error occurred while processing your request")
            .timestamp(LocalDateTime.now())
            .build();
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleGenericException(Exception ex) {
        log.error("Unexpected error: {}", ex.getMessage(), ex);
        return ErrorResponse.builder()
            .errorCode("INTERNAL_ERROR")
            .message("An unexpected error occurred")
            .timestamp(LocalDateTime.now())
            .build();
    }
}
```

## Retry Patterns

**Java (with exponential backoff):**
```java
@Service
@Slf4j
public class ExternalApiService {
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_DELAY_MS = 1000;

    public Response callExternalApi(Request request) {
        int attempt = 0;
        long delay = INITIAL_DELAY_MS;

        while (attempt < MAX_RETRIES) {
            try {
                log.debug("Calling external API, attempt: {}", attempt + 1);
                return performApiCall(request);
            } catch (TransientException e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    log.error("Max retries exceeded for external API call", e);
                    throw new ServiceException("External API call failed after retries", e);
                }

                log.warn("Transient error on attempt {}, retrying in {}ms", attempt, delay, e);
                sleep(delay);
                delay *= 2; // Exponential backoff
            }
        }

        throw new ServiceException("Unexpected error in retry logic");
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceException("Interrupted during retry", e);
        }
    }
}
```

## Circuit Breaker Pattern

**Java (Resilience4j):**
```java
@Service
@Slf4j
public class PaymentService {
    private final CircuitBreaker circuitBreaker;
    private final PaymentGateway paymentGateway;

    public PaymentService(PaymentGateway paymentGateway) {
        this.paymentGateway = paymentGateway;
        this.circuitBreaker = CircuitBreaker.of("paymentService",
            CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .slidingWindowSize(10)
                .build()
        );
    }

    public PaymentResponse processPayment(PaymentRequest request) {
        try {
            return circuitBreaker.executeSupplier(() -> {
                log.debug("Processing payment through gateway");
                return paymentGateway.process(request);
            });
        } catch (CallNotPermittedException e) {
            log.error("Circuit breaker is open, payment gateway unavailable");
            throw new ServiceException("Payment service temporarily unavailable", e);
        } catch (Exception e) {
            log.error("Error processing payment", e);
            throw new ServiceException("Payment processing failed", e);
        }
    }
}
```

## Best Practices Summary

1. **Always catch specific exceptions** - Never catch generic Exception unless at the top level
2. **Log before throwing** - Always log exceptions with context before re-throwing
3. **Don't swallow exceptions** - Always handle or propagate exceptions
4. **Use custom exceptions** - Create domain-specific exceptions for better error handling
5. **Add context** - Include relevant data in exceptions for debugging
6. **Validate early** - Check inputs at the entry point of your methods
7. **Use proper HTTP status codes** - Map exceptions to appropriate HTTP responses
8. **Don't expose internal details** - Sanitize error messages for external consumers
9. **Implement retry logic** - For transient failures in external services
10. **Use circuit breakers** - Protect your system from cascading failures
