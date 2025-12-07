# Test Data Patterns

## Overview

Test data patterns help create maintainable, readable test data. The three most common patterns are:

1. **Object Mother**: Predefined test objects
2. **Builder**: Fluent API for constructing test objects
3. **Factory**: Programmatic object creation with variations

## Object Mother Pattern

Create predefined test objects that represent common scenarios.

### When to Use
- Need consistent test data across multiple tests
- Working with complex domain objects
- Want to reduce duplication in test setup

### Python Example
```python
class UserMother:
    @staticmethod
    def create_default():
        return User(
            id=1,
            name="John Doe",
            email="john@example.com",
            role="user",
            is_active=True
        )

    @staticmethod
    def create_admin():
        return User(
            id=2,
            name="Admin User",
            email="admin@example.com",
            role="admin",
            is_active=True
        )

    @staticmethod
    def create_inactive():
        return User(
            id=3,
            name="Inactive User",
            email="inactive@example.com",
            role="user",
            is_active=False
        )

# Usage in tests
def test_admin_can_delete_users():
    admin = UserMother.create_admin()
    user = UserMother.create_default()

    result = admin.delete_user(user)

    assert result is True
```

### JavaScript Example
```javascript
class UserMother {
  static createDefault() {
    return {
      id: 1,
      name: 'John Doe',
      email: 'john@example.com',
      role: 'user',
      isActive: true
    };
  }

  static createAdmin() {
    return {
      id: 2,
      name: 'Admin User',
      email: 'admin@example.com',
      role: 'admin',
      isActive: true
    };
  }

  static createInactive() {
    return {
      id: 3,
      name: 'Inactive User',
      email: 'inactive@example.com',
      role: 'user',
      isActive: false
    };
  }
}

// Usage
test('admin can delete users', () => {
  const admin = UserMother.createAdmin();
  const user = UserMother.createDefault();

  const result = admin.deleteUser(user);

  expect(result).toBe(true);
});
```

### Java Example
```java
public class UserMother {
    public static User createDefault() {
        return new User(
            1L,
            "John Doe",
            "john@example.com",
            Role.USER,
            true
        );
    }

    public static User createAdmin() {
        return new User(
            2L,
            "Admin User",
            "admin@example.com",
            Role.ADMIN,
            true
        );
    }

    public static User createInactive() {
        return new User(
            3L,
            "Inactive User",
            "inactive@example.com",
            Role.USER,
            false
        );
    }
}

// Usage
@Test
void adminCanDeleteUsers() {
    User admin = UserMother.createAdmin();
    User user = UserMother.createDefault();

    boolean result = admin.deleteUser(user);

    assertTrue(result);
}
```

## Builder Pattern

Construct test objects with a fluent API, allowing customization of specific fields.

### When to Use
- Objects have many optional fields
- Need flexible test data construction
- Want to express test intent clearly
- Reduce test fragility by only specifying relevant fields

### Python Example
```python
class UserBuilder:
    def __init__(self):
        # Default values
        self._id = 1
        self._name = "Test User"
        self._email = "test@example.com"
        self._role = "user"
        self._is_active = True
        self._created_at = datetime.now()

    def with_id(self, id):
        self._id = id
        return self

    def with_name(self, name):
        self._name = name
        return self

    def with_email(self, email):
        self._email = email
        return self

    def with_role(self, role):
        self._role = role
        return self

    def inactive(self):
        self._is_active = False
        return self

    def created_at(self, date):
        self._created_at = date
        return self

    def build(self):
        return User(
            id=self._id,
            name=self._name,
            email=self._email,
            role=self._role,
            is_active=self._is_active,
            created_at=self._created_at
        )

# Usage
def test_inactive_users_cannot_login():
    user = (UserBuilder()
            .with_name("John")
            .with_email("john@example.com")
            .inactive()
            .build())

    result = login_service.login(user)

    assert result.success is False
    assert result.error == "Account is inactive"
```

### JavaScript Example
```javascript
class UserBuilder {
  constructor() {
    // Default values
    this._id = 1;
    this._name = 'Test User';
    this._email = 'test@example.com';
    this._role = 'user';
    this._isActive = true;
    this._createdAt = new Date();
  }

  withId(id) {
    this._id = id;
    return this;
  }

  withName(name) {
    this._name = name;
    return this;
  }

  withEmail(email) {
    this._email = email;
    return this;
  }

  withRole(role) {
    this._role = role;
    return this;
  }

  inactive() {
    this._isActive = false;
    return this;
  }

  createdAt(date) {
    this._createdAt = date;
    return this;
  }

  build() {
    return {
      id: this._id,
      name: this._name,
      email: this._email,
      role: this._role,
      isActive: this._isActive,
      createdAt: this._createdAt
    };
  }
}

// Usage
test('inactive users cannot login', () => {
  const user = new UserBuilder()
    .withName('John')
    .withEmail('john@example.com')
    .inactive()
    .build();

  const result = loginService.login(user);

  expect(result.success).toBe(false);
  expect(result.error).toBe('Account is inactive');
});
```

### Java Example
```java
public class UserBuilder {
    private Long id = 1L;
    private String name = "Test User";
    private String email = "test@example.com";
    private Role role = Role.USER;
    private boolean isActive = true;
    private LocalDateTime createdAt = LocalDateTime.now();

    public UserBuilder withId(Long id) {
        this.id = id;
        return this;
    }

    public UserBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public UserBuilder withEmail(String email) {
        this.email = email;
        return this;
    }

    public UserBuilder withRole(Role role) {
        this.role = role;
        return this;
    }

    public UserBuilder inactive() {
        this.isActive = false;
        return this;
    }

    public UserBuilder createdAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    public User build() {
        return new User(id, name, email, role, isActive, createdAt);
    }
}

// Usage
@Test
void inactiveUsersCannotLogin() {
    User user = new UserBuilder()
        .withName("John")
        .withEmail("john@example.com")
        .inactive()
        .build();

    LoginResult result = loginService.login(user);

    assertFalse(result.isSuccess());
    assertEquals("Account is inactive", result.getError());
}
```

## Factory Pattern

Programmatically create variations of test objects.

### When to Use
- Need to generate many similar objects
- Creating sequences or collections
- Need randomized or parameterized data

### Python Example
```python
class UserFactory:
    _id_counter = 1

    @classmethod
    def create(cls, **kwargs):
        """Create a user with optional field overrides"""
        defaults = {
            'id': cls._id_counter,
            'name': f"User {cls._id_counter}",
            'email': f"user{cls._id_counter}@example.com",
            'role': 'user',
            'is_active': True
        }
        cls._id_counter += 1

        # Merge defaults with provided kwargs
        user_data = {**defaults, **kwargs}
        return User(**user_data)

    @classmethod
    def create_batch(cls, count, **kwargs):
        """Create multiple users"""
        return [cls.create(**kwargs) for _ in range(count)]

    @classmethod
    def reset(cls):
        """Reset counter for test isolation"""
        cls._id_counter = 1

# Usage
def test_batch_user_creation():
    UserFactory.reset()

    users = UserFactory.create_batch(5, role='premium')

    assert len(users) == 5
    assert all(user.role == 'premium' for user in users)
    assert users[0].email == "user1@example.com"
    assert users[4].email == "user5@example.com"
```

### JavaScript Example
```javascript
class UserFactory {
  static idCounter = 1;

  static create(overrides = {}) {
    const defaults = {
      id: this.idCounter,
      name: `User ${this.idCounter}`,
      email: `user${this.idCounter}@example.com`,
      role: 'user',
      isActive: true
    };
    this.idCounter++;

    return { ...defaults, ...overrides };
  }

  static createBatch(count, overrides = {}) {
    return Array.from({ length: count }, () => this.create(overrides));
  }

  static reset() {
    this.idCounter = 1;
  }
}

// Usage
beforeEach(() => {
  UserFactory.reset();
});

test('batch user creation', () => {
  const users = UserFactory.createBatch(5, { role: 'premium' });

  expect(users).toHaveLength(5);
  expect(users.every(user => user.role === 'premium')).toBe(true);
  expect(users[0].email).toBe('user1@example.com');
  expect(users[4].email).toBe('user5@example.com');
});
```

### Java Example
```java
public class UserFactory {
    private static long idCounter = 1L;

    public static User create() {
        return create(Map.of());
    }

    public static User create(Map<String, Object> overrides) {
        long id = (long) overrides.getOrDefault("id", idCounter);
        String name = (String) overrides.getOrDefault("name", "User " + idCounter);
        String email = (String) overrides.getOrDefault("email", "user" + idCounter + "@example.com");
        Role role = (Role) overrides.getOrDefault("role", Role.USER);
        boolean isActive = (boolean) overrides.getOrDefault("isActive", true);

        idCounter++;

        return new User(id, name, email, role, isActive);
    }

    public static List<User> createBatch(int count, Map<String, Object> overrides) {
        return IntStream.range(0, count)
            .mapToObj(i -> create(overrides))
            .collect(Collectors.toList());
    }

    public static void reset() {
        idCounter = 1L;
    }
}

// Usage
@BeforeEach
void setUp() {
    UserFactory.reset();
}

@Test
void batchUserCreation() {
    List<User> users = UserFactory.createBatch(5, Map.of("role", Role.PREMIUM));

    assertEquals(5, users.size());
    assertTrue(users.stream().allMatch(user -> user.getRole() == Role.PREMIUM));
    assertEquals("user1@example.com", users.get(0).getEmail());
    assertEquals("user5@example.com", users.get(4).getEmail());
}
```

## Combining Patterns

Often, combining patterns provides the best flexibility.

### Python Example
```python
class OrderTestData:
    """Combines Object Mother and Builder patterns"""

    # Object Mother: Common scenarios
    @staticmethod
    def simple_order():
        return (OrderBuilder()
                .with_items([ItemMother.create_book()])
                .build())

    @staticmethod
    def large_order():
        return (OrderBuilder()
                .with_items(ItemFactory.create_batch(10))
                .build())

    @staticmethod
    def order_with_discount():
        return (OrderBuilder()
                .with_items([ItemMother.create_book()])
                .with_discount_code("SAVE20")
                .build())

# Usage
def test_large_orders_qualify_for_free_shipping():
    order = OrderTestData.large_order()

    shipping_cost = shipping_calculator.calculate(order)

    assert shipping_cost == 0.0
```

## Fixtures and Setup

### pytest Fixtures
```python
import pytest

@pytest.fixture
def default_user():
    """Provides a default user for tests"""
    return UserBuilder().build()

@pytest.fixture
def admin_user():
    """Provides an admin user for tests"""
    return UserBuilder().with_role('admin').build()

@pytest.fixture
def user_repository():
    """Provides an in-memory user repository"""
    return FakeUserRepository()

# Usage
def test_user_service_creates_user(user_repository):
    service = UserService(user_repository)
    user = UserBuilder().with_name("John").build()

    created_user = service.create(user)

    assert created_user.id is not None
```

### Jest Setup
```javascript
// test-helpers.js
export const setupTestUser = () => {
  return new UserBuilder()
    .withName('Test User')
    .build();
};

export const setupTestDatabase = () => {
  return new FakeDatabase();
};

// usage.test.js
import { setupTestUser, setupTestDatabase } from './test-helpers';

let db;

beforeEach(() => {
  db = setupTestDatabase();
});

test('creates user', () => {
  const user = setupTestUser();
  const service = new UserService(db);

  const created = service.create(user);

  expect(created.id).toBeDefined();
});
```

### JUnit Setup
```java
public abstract class UserServiceTestBase {
    protected UserRepository userRepository;
    protected UserService userService;

    @BeforeEach
    void setUp() {
        userRepository = new FakeUserRepository();
        userService = new UserService(userRepository);
    }

    protected User createDefaultUser() {
        return new UserBuilder().build();
    }

    protected User createAdminUser() {
        return new UserBuilder().withRole(Role.ADMIN).build();
    }
}

// Concrete test class
class UserServiceTest extends UserServiceTestBase {
    @Test
    void createsUser() {
        User user = createDefaultUser();

        User created = userService.create(user);

        assertNotNull(created.getId());
    }
}
```

## Randomized Test Data

For property-based testing or stress testing.

### Python with Faker
```python
from faker import Faker

class RandomUserFactory:
    def __init__(self):
        self.faker = Faker()

    def create(self):
        return User(
            id=self.faker.random_int(1, 100000),
            name=self.faker.name(),
            email=self.faker.email(),
            role=self.faker.random_element(['user', 'admin', 'premium']),
            is_active=self.faker.boolean()
        )

# Usage
def test_user_service_handles_random_data():
    factory = RandomUserFactory()

    for _ in range(100):
        user = factory.create()
        result = user_service.validate(user)
        assert result.is_valid or result.has_errors
```

### JavaScript with Faker
```javascript
import { faker } from '@faker-js/faker';

class RandomUserFactory {
  static create() {
    return {
      id: faker.number.int({ min: 1, max: 100000 }),
      name: faker.person.fullName(),
      email: faker.internet.email(),
      role: faker.helpers.arrayElement(['user', 'admin', 'premium']),
      isActive: faker.datatype.boolean()
    };
  }
}

// Usage
test('user service handles random data', () => {
  for (let i = 0; i < 100; i++) {
    const user = RandomUserFactory.create();
    const result = userService.validate(user);
    expect(result.isValid || result.hasErrors).toBe(true);
  }
});
```

## Best Practices

### 1. Keep Test Data Minimal
```python
# BAD - Too much irrelevant data
user = UserBuilder()
    .with_id(1)
    .with_name("John")
    .with_email("john@example.com")
    .with_address("123 Main St")
    .with_city("NYC")
    .with_zip("10001")
    .with_phone("555-1234")
    .with_preferences({"theme": "dark"})
    .build()

# Test only uses name
assert user.name == "John"

# GOOD - Only specify what matters
user = UserBuilder().with_name("John").build()
assert user.name == "John"
```

### 2. Make Test Intent Clear
```javascript
// BAD - Unclear why this specific data
const user = { id: 42, name: 'X', email: 'x@y.z' };

// GOOD - Clear intent
const userWithInvalidEmail = new UserBuilder()
  .withEmail('invalid-email')
  .build();
```

### 3. Isolate Test Data
```python
# BAD - Shared mutable data
SHARED_USER = User(name="Shared")

def test_one():
    SHARED_USER.name = "Modified"  # Affects other tests!

# GOOD - Fresh data per test
@pytest.fixture
def user():
    return UserBuilder().build()

def test_one(user):
    user.name = "Modified"  # Isolated
```

### 4. Use Factories for Collections
```java
// GOOD - Easy to generate test collections
List<User> users = UserFactory.createBatch(100);
List<Order> orders = OrderFactory.createBatchForUser(user, 10);
```

### 5. Document Common Scenarios
```python
class OrderTestData:
    """
    Common order scenarios for testing:
    - new_order(): Unpaid, unshipped
    - paid_order(): Paid, unshipped
    - shipped_order(): Paid, shipped
    - cancelled_order(): Cancelled at any stage
    """

    @staticmethod
    def new_order():
        """New order, unpaid and unshipped"""
        return OrderBuilder().with_status('new').build()
```

## Anti-Patterns to Avoid

### Test Data Overuse
```python
# BAD - Building complex object for simple test
def test_string_uppercase():
    user = UserBuilder().with_name("john").build()
    assert user.name.upper() == "JOHN"

# GOOD - Use simple data
def test_string_uppercase():
    name = "john"
    assert name.upper() == "JOHN"
```

### Brittle Test Data
```javascript
// BAD - Hardcoded IDs that might conflict
const user = { id: 1, name: 'John' };

// GOOD - Use factory with auto-incrementing IDs
const user = UserFactory.create({ name: 'John' });
```

### Hidden Dependencies
```python
# BAD - Builder depends on global state
class UserBuilder:
    def build(self):
        return User(timezone=GLOBAL_TIMEZONE)  # Hidden dependency!

# GOOD - Explicit dependencies
class UserBuilder:
    def __init__(self, timezone='UTC'):
        self.timezone = timezone

    def build(self):
        return User(timezone=self.timezone)
```
