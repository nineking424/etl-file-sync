# Coding Standards

## SOLID Principles

### Single Responsibility Principle (SRP)
A class should have only one reason to change.

**Good Example:**
```java
// Each class has a single responsibility
public class UserValidator {
    public boolean isValid(User user) {
        return user.getEmail() != null && user.getName() != null;
    }
}

public class UserRepository {
    public User save(User user) {
        // Database operations only
    }
}

public class UserService {
    private final UserValidator validator;
    private final UserRepository repository;

    public User createUser(User user) {
        if (!validator.isValid(user)) {
            throw new ValidationException("Invalid user");
        }
        return repository.save(user);
    }
}
```

**Bad Example:**
```java
// Class doing too many things
public class UserManager {
    public User createUser(User user) {
        // Validation
        if (user.getEmail() == null) throw new Exception();
        // Database
        db.save(user);
        // Email sending
        emailService.send(user.getEmail());
        // Logging
        log.info("User created");
        return user;
    }
}
```

### Open/Closed Principle (OCP)
Open for extension, closed for modification.

**Good Example:**
```python
from abc import ABC, abstractmethod

class PaymentProcessor(ABC):
    @abstractmethod
    def process(self, amount: float) -> bool:
        pass

class CreditCardProcessor(PaymentProcessor):
    def process(self, amount: float) -> bool:
        # Credit card logic
        return True

class PayPalProcessor(PaymentProcessor):
    def process(self, amount: float) -> bool:
        # PayPal logic
        return True

# Easy to add new payment methods without modifying existing code
class BitcoinProcessor(PaymentProcessor):
    def process(self, amount: float) -> bool:
        # Bitcoin logic
        return True
```

### Liskov Substitution Principle (LSP)
Objects of a superclass should be replaceable with objects of subclasses.

**Good Example:**
```kotlin
interface Bird {
    fun eat()
}

interface FlyingBird : Bird {
    fun fly()
}

class Sparrow : FlyingBird {
    override fun eat() { /* ... */ }
    override fun fly() { /* ... */ }
}

class Penguin : Bird {
    override fun eat() { /* ... */ }
    // Doesn't implement fly() - correct!
}
```

### Interface Segregation Principle (ISP)
Clients should not be forced to depend on interfaces they don't use.

**Good Example:**
```java
// Segregated interfaces
public interface Readable {
    String read();
}

public interface Writable {
    void write(String data);
}

public interface Deletable {
    void delete();
}

// Implement only what you need
public class ReadOnlyFile implements Readable {
    public String read() { /* ... */ }
}

public class ReadWriteFile implements Readable, Writable {
    public String read() { /* ... */ }
    public void write(String data) { /* ... */ }
}
```

### Dependency Inversion Principle (DIP)
Depend on abstractions, not concretions.

**Good Example:**
```python
# Abstraction
class Database(ABC):
    @abstractmethod
    def save(self, data: dict) -> bool:
        pass

# Concrete implementations
class PostgresDatabase(Database):
    def save(self, data: dict) -> bool:
        # Postgres-specific logic
        return True

class MongoDatabase(Database):
    def save(self, data: dict) -> bool:
        # MongoDB-specific logic
        return True

# Service depends on abstraction, not concrete implementation
class UserService:
    def __init__(self, database: Database):
        self._database = database

    def create_user(self, user_data: dict) -> bool:
        return self._database.save(user_data)
```

## Language-Specific Conventions

### Java Conventions

#### Naming
```java
// Classes: PascalCase
public class UserService { }

// Interfaces: PascalCase with descriptive names
public interface UserRepository { }

// Methods: camelCase, verb-based
public void createUser() { }
public User findUserById(Long id) { }

// Variables: camelCase
private String userName;
private List<User> activeUsers;

// Constants: UPPER_SNAKE_CASE
public static final int MAX_RETRY_ATTEMPTS = 3;
public static final String DEFAULT_ENCODING = "UTF-8";

// Packages: lowercase
package com.company.product.module;
```

#### Code Organization
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {
    // 1. Constants
    private static final int MAX_USERS = 100;

    // 2. Dependencies (final fields)
    private final UserRepository userRepository;
    private final EmailService emailService;

    // 3. Public methods
    public User createUser(UserDto dto) {
        // Implementation
    }

    // 4. Private methods
    private void validateUser(User user) {
        // Validation logic
    }

    // 5. Inner classes (if needed)
    private static class UserBuilder {
        // Builder implementation
    }
}
```

#### Best Practices
```java
// Use Optional for nullable returns
public Optional<User> findUserById(Long id) {
    return userRepository.findById(id);
}

// Use Streams for collection operations
List<String> names = users.stream()
    .filter(User::isActive)
    .map(User::getName)
    .collect(Collectors.toList());

// Use try-with-resources
try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
    return reader.readLine();
}

// Use StringBuilder for string concatenation in loops
StringBuilder result = new StringBuilder();
for (String item : items) {
    result.append(item).append(", ");
}
```

### Python Conventions

#### Naming (PEP 8)
```python
# Classes: PascalCase
class UserService:
    pass

# Functions/Methods: snake_case
def create_user():
    pass

def find_user_by_id(user_id: int):
    pass

# Variables: snake_case
user_name = "John"
active_users = []

# Constants: UPPER_SNAKE_CASE
MAX_RETRY_ATTEMPTS = 3
DEFAULT_ENCODING = "utf-8"

# Private members: leading underscore
class User:
    def __init__(self):
        self._private_field = None

    def _private_method(self):
        pass
```

#### Code Organization
```python
# 1. Module docstring
"""
User service module.

This module provides user management functionality.
"""

# 2. Imports (standard library, third-party, local)
import logging
from typing import Optional, List

from sqlalchemy import create_engine

from .models import User
from .repositories import UserRepository

# 3. Constants
MAX_USERS = 100
DEFAULT_PAGE_SIZE = 20

# 4. Class definitions
class UserService:
    """Service for managing users."""

    def __init__(self, user_repository: UserRepository):
        self._user_repository = user_repository
        self._logger = logging.getLogger(__name__)

    def create_user(self, user_data: dict) -> User:
        """Create a new user."""
        # Implementation

    def _validate_user(self, user: User) -> bool:
        """Private validation method."""
        # Validation logic
```

#### Best Practices
```python
# Use type hints
def find_user_by_id(user_id: int) -> Optional[User]:
    return self._repository.find_by_id(user_id)

# Use dataclasses for data models
from dataclasses import dataclass

@dataclass
class User:
    id: int
    name: str
    email: str

# Use context managers
with open('file.txt', 'r') as f:
    content = f.read()

# Use list comprehensions
active_users = [user for user in users if user.is_active]

# Use f-strings for formatting
message = f"User {user.name} created with ID {user.id}"
```

### Kotlin Conventions

#### Naming
```kotlin
// Classes: PascalCase
class UserService

// Functions: camelCase
fun createUser()
fun findUserById(id: Long)

// Properties: camelCase
val userName: String
var activeUsers: List<User>

// Constants: UPPER_SNAKE_CASE
const val MAX_RETRY_ATTEMPTS = 3
const val DEFAULT_ENCODING = "UTF-8"

// Packages: lowercase
package com.company.product.module
```

#### Code Organization
```kotlin
class UserService(
    private val userRepository: UserRepository,
    private val emailService: EmailService
) {
    companion object {
        private const val MAX_USERS = 100
    }

    fun createUser(dto: UserDto): User {
        // Implementation
    }

    private fun validateUser(user: User) {
        // Validation logic
    }
}
```

#### Best Practices
```kotlin
// Use data classes
data class User(
    val id: Long,
    val name: String,
    val email: String
)

// Use nullable types
fun findUserById(id: Long): User? {
    return userRepository.findById(id)
}

// Use extension functions
fun String.isValidEmail(): Boolean {
    return this.contains("@")
}

// Use when expressions
fun getStatus(code: Int): String = when (code) {
    200 -> "OK"
    404 -> "Not Found"
    500 -> "Server Error"
    else -> "Unknown"
}

// Use sealed classes for state
sealed class Result<out T> {
    data class Success<T>(val data: T) : Result<T>()
    data class Error(val message: String) : Result<Nothing>()
}
```

## Code Quality Standards

### Readability
- Use meaningful variable and method names
- Keep methods short (max 20-30 lines)
- Use comments for complex logic only
- Avoid deep nesting (max 3 levels)

### Maintainability
- Follow DRY (Don't Repeat Yourself)
- Use proper abstraction layers
- Keep dependencies minimal
- Write self-documenting code

### Performance
- Avoid premature optimization
- Use appropriate data structures
- Cache expensive operations when needed
- Profile before optimizing

### Testing
- Write testable code
- Use dependency injection
- Avoid static methods for business logic
- Keep side effects minimal

## Documentation Standards

### Java (JavaDoc)
```java
/**
 * Creates a new user in the system.
 *
 * @param userDto the user data transfer object containing user information
 * @return the created user entity
 * @throws ValidationException if the user data is invalid
 * @throws ServiceException if the creation fails
 */
public User createUser(UserDto userDto) {
    // Implementation
}
```

### Python (Docstrings)
```python
def create_user(self, user_data: dict) -> User:
    """
    Create a new user in the system.

    Args:
        user_data: Dictionary containing user information

    Returns:
        User: The created user entity

    Raises:
        ValidationException: If the user data is invalid
        ServiceException: If the creation fails
    """
    # Implementation
```

### Kotlin (KDoc)
```kotlin
/**
 * Creates a new user in the system.
 *
 * @param userDto the user data transfer object containing user information
 * @return the created user entity
 * @throws ValidationException if the user data is invalid
 * @throws ServiceException if the creation fails
 */
fun createUser(userDto: UserDto): User {
    // Implementation
}
```
