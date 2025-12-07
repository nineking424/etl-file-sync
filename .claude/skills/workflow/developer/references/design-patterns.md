# Design Patterns

## Creational Patterns

### Singleton Pattern
Ensures a class has only one instance and provides a global point of access.

**Java Implementation:**
```java
@Component
public class ConfigurationManager {
    private static ConfigurationManager instance;
    private final Map<String, String> config = new HashMap<>();

    private ConfigurationManager() {
        // Private constructor
        loadConfiguration();
    }

    public static synchronized ConfigurationManager getInstance() {
        if (instance == null) {
            instance = new ConfigurationManager();
        }
        return instance;
    }

    public String get(String key) {
        return config.get(key);
    }

    private void loadConfiguration() {
        // Load configuration from file or environment
    }
}
```

**Python Implementation:**
```python
class ConfigurationManager:
    _instance = None
    _config: Dict[str, str] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_configuration()
        return cls._instance

    def get(self, key: str) -> Optional[str]:
        return self._config.get(key)

    def _load_configuration(self):
        # Load configuration from file or environment
        pass
```

### Factory Pattern
Creates objects without specifying the exact class to create.

**Java Implementation:**
```java
public interface PaymentProcessor {
    boolean process(Payment payment);
}

@Component
public class CreditCardProcessor implements PaymentProcessor {
    @Override
    public boolean process(Payment payment) {
        // Credit card processing logic
        return true;
    }
}

@Component
public class PayPalProcessor implements PaymentProcessor {
    @Override
    public boolean process(Payment payment) {
        // PayPal processing logic
        return true;
    }
}

@Component
public class PaymentProcessorFactory {
    public PaymentProcessor createProcessor(PaymentMethod method) {
        return switch (method) {
            case CREDIT_CARD -> new CreditCardProcessor();
            case PAYPAL -> new PayPalProcessor();
            case BANK_TRANSFER -> new BankTransferProcessor();
            default -> throw new IllegalArgumentException("Unknown payment method: " + method);
        };
    }
}
```

**Kotlin Implementation:**
```kotlin
interface PaymentProcessor {
    fun process(payment: Payment): Boolean
}

class CreditCardProcessor : PaymentProcessor {
    override fun process(payment: Payment): Boolean {
        // Credit card processing logic
        return true
    }
}

class PayPalProcessor : PaymentProcessor {
    override fun process(payment: Payment): Boolean {
        // PayPal processing logic
        return true
    }
}

object PaymentProcessorFactory {
    fun createProcessor(method: PaymentMethod): PaymentProcessor = when (method) {
        PaymentMethod.CREDIT_CARD -> CreditCardProcessor()
        PaymentMethod.PAYPAL -> PayPalProcessor()
        PaymentMethod.BANK_TRANSFER -> BankTransferProcessor()
    }
}
```

### Builder Pattern
Constructs complex objects step by step.

**Java Implementation:**
```java
@Data
@Builder
public class User {
    private Long id;
    private String name;
    private String email;
    private String phone;
    private Address address;
    private LocalDateTime createdAt;
}

// Usage
User user = User.builder()
    .name("John Doe")
    .email("john@example.com")
    .phone("+1234567890")
    .address(address)
    .createdAt(LocalDateTime.now())
    .build();
```

**Python Implementation:**
```python
from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class User:
    name: str
    email: str
    phone: Optional[str] = None
    address: Optional['Address'] = None
    created_at: datetime = None

    @classmethod
    def builder(cls):
        return UserBuilder()

class UserBuilder:
    def __init__(self):
        self._name = None
        self._email = None
        self._phone = None
        self._address = None
        self._created_at = None

    def name(self, name: str):
        self._name = name
        return self

    def email(self, email: str):
        self._email = email
        return self

    def phone(self, phone: str):
        self._phone = phone
        return self

    def address(self, address: 'Address'):
        self._address = address
        return self

    def build(self) -> User:
        return User(
            name=self._name,
            email=self._email,
            phone=self._phone,
            address=self._address,
            created_at=datetime.now()
        )

# Usage
user = User.builder() \
    .name("John Doe") \
    .email("john@example.com") \
    .phone("+1234567890") \
    .build()
```

## Structural Patterns

### Repository Pattern
Mediates between domain and data mapping layers.

**Java Implementation:**
```java
public interface UserRepository extends JpaRepository<User, Long> {
    Optional<User> findByEmail(String email);
    List<User> findByStatus(UserStatus status);

    @Query("SELECT u FROM User u WHERE u.createdAt > :date")
    List<User> findRecentUsers(@Param("date") LocalDateTime date);
}

@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {
    private final UserRepository userRepository;

    public User createUser(User user) {
        log.debug("Creating user: {}", user.getEmail());
        return userRepository.save(user);
    }

    public Optional<User> findUserById(Long id) {
        log.debug("Finding user by id: {}", id);
        return userRepository.findById(id);
    }

    public List<User> findActiveUsers() {
        log.debug("Finding active users");
        return userRepository.findByStatus(UserStatus.ACTIVE);
    }
}
```

**Python Implementation:**
```python
from abc import ABC, abstractmethod
from typing import List, Optional

class UserRepository(ABC):
    @abstractmethod
    def save(self, user: User) -> User:
        pass

    @abstractmethod
    def find_by_id(self, user_id: int) -> Optional[User]:
        pass

    @abstractmethod
    def find_by_email(self, email: str) -> Optional[User]:
        pass

    @abstractmethod
    def find_by_status(self, status: str) -> List[User]:
        pass

class SqlAlchemyUserRepository(UserRepository):
    def __init__(self, session):
        self._session = session

    def save(self, user: User) -> User:
        self._session.add(user)
        self._session.commit()
        return user

    def find_by_id(self, user_id: int) -> Optional[User]:
        return self._session.query(User).filter_by(id=user_id).first()

    def find_by_email(self, email: str) -> Optional[User]:
        return self._session.query(User).filter_by(email=email).first()

    def find_by_status(self, status: str) -> List[User]:
        return self._session.query(User).filter_by(status=status).all()

class UserService:
    def __init__(self, user_repository: UserRepository):
        self._user_repository = user_repository
        self._logger = logging.getLogger(__name__)

    def create_user(self, user: User) -> User:
        self._logger.debug(f"Creating user: {user.email}")
        return self._user_repository.save(user)

    def find_user_by_id(self, user_id: int) -> Optional[User]:
        self._logger.debug(f"Finding user by id: {user_id}")
        return self._user_repository.find_by_id(user_id)
```

### Adapter Pattern
Allows incompatible interfaces to work together.

**Java Implementation:**
```java
// External library interface
public interface LegacyPaymentGateway {
    void makePayment(String account, double amount);
}

// Our application interface
public interface PaymentProcessor {
    boolean processPayment(Payment payment);
}

// Adapter
@Component
public class LegacyPaymentAdapter implements PaymentProcessor {
    private final LegacyPaymentGateway legacyGateway;

    public LegacyPaymentAdapter(LegacyPaymentGateway legacyGateway) {
        this.legacyGateway = legacyGateway;
    }

    @Override
    public boolean processPayment(Payment payment) {
        try {
            legacyGateway.makePayment(
                payment.getAccountNumber(),
                payment.getAmount()
            );
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
```

### Decorator Pattern
Adds new functionality to objects dynamically.

**Kotlin Implementation:**
```kotlin
interface Coffee {
    fun cost(): Double
    fun description(): String
}

class SimpleCoffee : Coffee {
    override fun cost() = 2.0
    override fun description() = "Simple coffee"
}

abstract class CoffeeDecorator(private val coffee: Coffee) : Coffee {
    override fun cost() = coffee.cost()
    override fun description() = coffee.description()
}

class Milk(coffee: Coffee) : CoffeeDecorator(coffee) {
    override fun cost() = super.cost() + 0.5
    override fun description() = "${super.description()}, milk"
}

class Sugar(coffee: Coffee) : CoffeeDecorator(coffee) {
    override fun cost() = super.cost() + 0.2
    override fun description() = "${super.description()}, sugar"
}

// Usage
val coffee = Sugar(Milk(SimpleCoffee()))
println("${coffee.description()} costs ${coffee.cost()}")
// Output: Simple coffee, milk, sugar costs 2.7
```

## Behavioral Patterns

### Strategy Pattern
Defines a family of algorithms and makes them interchangeable.

**Java Implementation:**
```java
public interface SortingStrategy {
    <T extends Comparable<T>> List<T> sort(List<T> data);
}

@Component
public class QuickSortStrategy implements SortingStrategy {
    @Override
    public <T extends Comparable<T>> List<T> sort(List<T> data) {
        // QuickSort implementation
        return data;
    }
}

@Component
public class MergeSortStrategy implements SortingStrategy {
    @Override
    public <T extends Comparable<T>> List<T> sort(List<T> data) {
        // MergeSort implementation
        return data;
    }
}

@Service
public class DataProcessor {
    private SortingStrategy sortingStrategy;

    public void setSortingStrategy(SortingStrategy strategy) {
        this.sortingStrategy = strategy;
    }

    public <T extends Comparable<T>> List<T> processData(List<T> data) {
        return sortingStrategy.sort(data);
    }
}
```

**Python Implementation:**
```python
from abc import ABC, abstractmethod
from typing import List, TypeVar

T = TypeVar('T')

class SortingStrategy(ABC):
    @abstractmethod
    def sort(self, data: List[T]) -> List[T]:
        pass

class QuickSortStrategy(SortingStrategy):
    def sort(self, data: List[T]) -> List[T]:
        # QuickSort implementation
        return sorted(data)

class MergeSortStrategy(SortingStrategy):
    def sort(self, data: List[T]) -> List[T]:
        # MergeSort implementation
        return sorted(data)

class DataProcessor:
    def __init__(self, sorting_strategy: SortingStrategy):
        self._sorting_strategy = sorting_strategy

    def set_sorting_strategy(self, strategy: SortingStrategy):
        self._sorting_strategy = strategy

    def process_data(self, data: List[T]) -> List[T]:
        return self._sorting_strategy.sort(data)
```

### Observer Pattern
Defines one-to-many dependency between objects.

**Java Implementation:**
```java
public interface EventListener {
    void update(Event event);
}

@Component
public class EventPublisher {
    private final List<EventListener> listeners = new ArrayList<>();

    public void subscribe(EventListener listener) {
        listeners.add(listener);
    }

    public void unsubscribe(EventListener listener) {
        listeners.remove(listener);
    }

    public void notifyListeners(Event event) {
        listeners.forEach(listener -> listener.update(event));
    }
}

@Component
public class EmailNotificationListener implements EventListener {
    @Override
    public void update(Event event) {
        // Send email notification
        System.out.println("Email sent for event: " + event.getType());
    }
}

@Component
public class LoggingListener implements EventListener {
    @Override
    public void update(Event event) {
        // Log event
        System.out.println("Event logged: " + event.getType());
    }
}
```

### Template Method Pattern
Defines skeleton of algorithm, letting subclasses override steps.

**Kotlin Implementation:**
```kotlin
abstract class DataProcessor {
    fun process(data: String): String {
        val validated = validate(data)
        val transformed = transform(validated)
        val saved = save(transformed)
        return saved
    }

    protected abstract fun validate(data: String): String
    protected abstract fun transform(data: String): String
    protected abstract fun save(data: String): String
}

class CsvDataProcessor : DataProcessor() {
    override fun validate(data: String): String {
        // CSV validation logic
        return data
    }

    override fun transform(data: String): String {
        // CSV transformation logic
        return data.uppercase()
    }

    override fun save(data: String): String {
        // Save to CSV file
        return data
    }
}

class JsonDataProcessor : DataProcessor() {
    override fun validate(data: String): String {
        // JSON validation logic
        return data
    }

    override fun transform(data: String): String {
        // JSON transformation logic
        return data.lowercase()
    }

    override fun save(data: String): String {
        // Save to JSON file
        return data
    }
}
```

### Chain of Responsibility Pattern
Passes request along a chain of handlers.

**Java Implementation:**
```java
public abstract class AuthenticationHandler {
    private AuthenticationHandler next;

    public AuthenticationHandler setNext(AuthenticationHandler handler) {
        this.next = handler;
        return handler;
    }

    public abstract boolean handle(AuthRequest request);

    protected boolean handleNext(AuthRequest request) {
        if (next == null) {
            return true;
        }
        return next.handle(request);
    }
}

@Component
public class RateLimitHandler extends AuthenticationHandler {
    @Override
    public boolean handle(AuthRequest request) {
        if (isRateLimited(request)) {
            return false;
        }
        return handleNext(request);
    }

    private boolean isRateLimited(AuthRequest request) {
        // Check rate limiting
        return false;
    }
}

@Component
public class AuthenticationValidationHandler extends AuthenticationHandler {
    @Override
    public boolean handle(AuthRequest request) {
        if (!isValidCredentials(request)) {
            return false;
        }
        return handleNext(request);
    }

    private boolean isValidCredentials(AuthRequest request) {
        // Validate credentials
        return true;
    }
}

@Component
public class AuthorizationHandler extends AuthenticationHandler {
    @Override
    public boolean handle(AuthRequest request) {
        if (!hasPermission(request)) {
            return false;
        }
        return handleNext(request);
    }

    private boolean hasPermission(AuthRequest request) {
        // Check permissions
        return true;
    }
}

// Usage
AuthenticationHandler chain = new RateLimitHandler();
chain.setNext(new AuthenticationValidationHandler())
     .setNext(new AuthorizationHandler());

boolean result = chain.handle(request);
```

## Dependency Injection Patterns

### Constructor Injection (Recommended)
```java
@Service
@RequiredArgsConstructor
public class UserService {
    private final UserRepository userRepository;
    private final EmailService emailService;
    private final ValidationService validationService;

    // All dependencies injected via constructor
}
```

### Field Injection (Not Recommended)
```java
@Service
public class UserService {
    @Autowired
    private UserRepository userRepository;  // Avoid this

    @Autowired
    private EmailService emailService;  // Avoid this
}
```

### Setter Injection (For Optional Dependencies)
```java
@Service
public class UserService {
    private final UserRepository userRepository;
    private NotificationService notificationService;  // Optional

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Autowired(required = false)
    public void setNotificationService(NotificationService service) {
        this.notificationService = service;
    }
}
```

## When to Use Which Pattern

| Pattern | Use Case |
|---------|----------|
| Singleton | Global configuration, logging, caching |
| Factory | Object creation with complex logic |
| Builder | Objects with many optional parameters |
| Repository | Data access abstraction |
| Adapter | Integrate with legacy or third-party systems |
| Decorator | Add functionality without modifying original class |
| Strategy | Multiple algorithms for same operation |
| Observer | Event-driven systems, notifications |
| Template Method | Common algorithm with variable steps |
| Chain of Responsibility | Request processing pipeline |
