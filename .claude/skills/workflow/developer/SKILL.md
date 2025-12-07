---
name: developer
description: Production-quality backend code generation with industry best practices, design patterns, and comprehensive error handling
---

# Developer Skill

Generate production-ready backend code following industry best practices, design patterns, and comprehensive error handling strategies.

## Core Workflow

### 1. Requirements Analysis
- Understand the business requirement and technical context
- Identify appropriate design patterns
- Determine error handling strategy
- Plan dependency injection approach

### 2. Code Generation
- Follow language-specific coding standards
- Implement appropriate design patterns
- Include comprehensive error handling
- Add structured logging
- Write clean, maintainable code

### 3. Quality Assurance
- Ensure SOLID principles are followed
- Verify proper dependency injection
- Validate error handling coverage
- Check logging completeness

## Language-Specific Guidelines

### Java
- Use Spring Framework conventions for dependency injection
- Follow Java naming conventions (PascalCase for classes, camelCase for methods/variables)
- Implement interfaces for testability
- Use Optional<T> for nullable returns
- Leverage Java Streams for collection operations
- Apply Lombok annotations to reduce boilerplate
- Use SLF4J for logging

**Example Structure:**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {
    private final UserRepository userRepository;

    public Optional<User> findUserById(Long id) {
        try {
            log.debug("Finding user by id: {}", id);
            return userRepository.findById(id);
        } catch (DataAccessException e) {
            log.error("Database error while finding user: {}", id, e);
            throw new ServiceException("Failed to retrieve user", e);
        }
    }
}
```

### Python
- Use type hints for all function signatures
- Follow PEP 8 style guidelines
- Implement dataclasses for data models
- Use dependency injection with dependency-injector or similar
- Apply context managers for resource management
- Use logging module with structured logging

**Example Structure:**
```python
from typing import Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class User:
    id: int
    name: str
    email: str

class UserService:
    def __init__(self, user_repository: UserRepository):
        self._user_repository = user_repository

    def find_user_by_id(self, user_id: int) -> Optional[User]:
        try:
            logger.debug(f"Finding user by id: {user_id}")
            return self._user_repository.find_by_id(user_id)
        except DatabaseError as e:
            logger.error(f"Database error while finding user: {user_id}", exc_info=True)
            raise ServiceException("Failed to retrieve user") from e
```

### Kotlin
- Use data classes for models
- Leverage sealed classes for state representation
- Apply extension functions for utility methods
- Use coroutines for asynchronous operations
- Implement dependency injection with Koin or Dagger
- Use companion objects for factory methods

**Example Structure:**
```kotlin
@Service
class UserService(
    private val userRepository: UserRepository,
    private val logger: Logger = LoggerFactory.getLogger(UserService::class.java)
) {
    fun findUserById(id: Long): User? {
        return try {
            logger.debug { "Finding user by id: $id" }
            userRepository.findById(id)
        } catch (e: DataAccessException) {
            logger.error(e) { "Database error while finding user: $id" }
            throw ServiceException("Failed to retrieve user", e)
        }
    }
}
```

## Design Pattern Integration

### Repository Pattern
- Separate data access logic from business logic
- Abstract database operations behind interfaces
- Enable easy testing with mock repositories

### Service Layer Pattern
- Encapsulate business logic in service classes
- Coordinate between repositories and controllers
- Handle transaction boundaries

### Factory Pattern
- Create complex objects with factory methods
- Encapsulate object creation logic
- Support different creation strategies

### Strategy Pattern
- Define family of algorithms
- Make algorithms interchangeable
- Enable runtime selection of algorithm

### Dependency Injection
- Constructor injection for required dependencies
- Setter injection for optional dependencies
- Interface-based dependencies for testability

See `references/design-patterns.md` for detailed examples.

## Error Handling Automation

### Exception Hierarchy
- Create custom exception classes for domain-specific errors
- Extend from appropriate base exceptions
- Include meaningful error messages and context

### Try-Catch Patterns
- Catch specific exceptions, not generic ones
- Log exceptions with appropriate severity
- Re-throw or wrap exceptions appropriately
- Always include context in error messages

### Validation
- Validate input parameters early
- Use assertion libraries for complex validation
- Return meaningful error messages to callers

See `references/error-handling.md` for comprehensive strategies.

## Logging Best Practices

### Structured Logging
- Use consistent log levels (DEBUG, INFO, WARN, ERROR)
- Include contextual information (user ID, transaction ID, etc.)
- Use parameterized logging for performance
- Avoid logging sensitive information (passwords, tokens, PII)

### Log Levels
- **DEBUG**: Detailed diagnostic information
- **INFO**: General informational messages
- **WARN**: Warning messages for potentially harmful situations
- **ERROR**: Error events that might still allow the application to continue

### Logging Points
- Log method entry/exit for critical operations
- Log all exceptions with stack traces
- Log important state changes
- Log external service calls

See `references/logging-conventions.md` for detailed guidelines.

## Code Generation Checklist

- [ ] Appropriate design pattern selected and implemented
- [ ] Dependency injection properly configured
- [ ] Comprehensive error handling in place
- [ ] Structured logging added
- [ ] SOLID principles followed
- [ ] Code follows language-specific conventions
- [ ] Input validation implemented
- [ ] Documentation comments added
- [ ] Type safety ensured
- [ ] Resource management handled (auto-close, context managers)

## Usage Examples

### Example 1: Create a Service with Repository Pattern

**User Request:** "Create a UserService that retrieves users by ID and email"

**Generated Code:**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {
    private final UserRepository userRepository;

    public Optional<User> findById(Long id) {
        if (id == null || id <= 0) {
            throw new IllegalArgumentException("User ID must be positive");
        }

        try {
            log.debug("Finding user by id: {}", id);
            Optional<User> user = userRepository.findById(id);

            if (user.isPresent()) {
                log.info("User found: {}", id);
            } else {
                log.debug("User not found: {}", id);
            }

            return user;
        } catch (DataAccessException e) {
            log.error("Database error while finding user by id: {}", id, e);
            throw new ServiceException("Failed to retrieve user", e);
        }
    }

    public Optional<User> findByEmail(String email) {
        if (email == null || email.isBlank()) {
            throw new IllegalArgumentException("Email cannot be empty");
        }

        try {
            log.debug("Finding user by email: {}", email);
            Optional<User> user = userRepository.findByEmail(email);

            if (user.isPresent()) {
                log.info("User found by email: {}", email);
            } else {
                log.debug("User not found by email: {}", email);
            }

            return user;
        } catch (DataAccessException e) {
            log.error("Database error while finding user by email: {}", email, e);
            throw new ServiceException("Failed to retrieve user", e);
        }
    }
}
```

### Example 2: Create a Factory Pattern Implementation

**User Request:** "Create a payment processor factory that supports multiple payment methods"

**Generated Code:**
```python
from abc import ABC, abstractmethod
from typing import Dict, Type
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class PaymentMethod(Enum):
    CREDIT_CARD = "credit_card"
    PAYPAL = "paypal"
    BANK_TRANSFER = "bank_transfer"

class PaymentProcessor(ABC):
    @abstractmethod
    def process_payment(self, amount: float, **kwargs) -> bool:
        pass

class CreditCardProcessor(PaymentProcessor):
    def process_payment(self, amount: float, **kwargs) -> bool:
        try:
            logger.info(f"Processing credit card payment: ${amount}")
            # Implementation here
            return True
        except Exception as e:
            logger.error(f"Credit card payment failed: ${amount}", exc_info=True)
            raise PaymentException("Credit card payment failed") from e

class PayPalProcessor(PaymentProcessor):
    def process_payment(self, amount: float, **kwargs) -> bool:
        try:
            logger.info(f"Processing PayPal payment: ${amount}")
            # Implementation here
            return True
        except Exception as e:
            logger.error(f"PayPal payment failed: ${amount}", exc_info=True)
            raise PaymentException("PayPal payment failed") from e

class BankTransferProcessor(PaymentProcessor):
    def process_payment(self, amount: float, **kwargs) -> bool:
        try:
            logger.info(f"Processing bank transfer: ${amount}")
            # Implementation here
            return True
        except Exception as e:
            logger.error(f"Bank transfer failed: ${amount}", exc_info=True)
            raise PaymentException("Bank transfer failed") from e

class PaymentProcessorFactory:
    _processors: Dict[PaymentMethod, Type[PaymentProcessor]] = {
        PaymentMethod.CREDIT_CARD: CreditCardProcessor,
        PaymentMethod.PAYPAL: PayPalProcessor,
        PaymentMethod.BANK_TRANSFER: BankTransferProcessor,
    }

    @classmethod
    def create(cls, method: PaymentMethod) -> PaymentProcessor:
        try:
            logger.debug(f"Creating payment processor: {method.value}")
            processor_class = cls._processors.get(method)

            if processor_class is None:
                raise ValueError(f"Unsupported payment method: {method.value}")

            return processor_class()
        except Exception as e:
            logger.error(f"Failed to create payment processor: {method.value}", exc_info=True)
            raise PaymentException("Failed to create payment processor") from e

class PaymentException(Exception):
    pass
```

## Integration with Other Skills

- **tester**: Generate unit tests for the created code
- **reviewer**: Review code quality and suggest improvements
- **documenter**: Generate API documentation
- **refactor**: Optimize and refactor existing code

## References

- See `references/coding-standards.md` for detailed coding conventions
- See `references/design-patterns.md` for pattern implementations
- See `references/error-handling.md` for exception handling strategies
- See `references/logging-conventions.md` for logging best practices
