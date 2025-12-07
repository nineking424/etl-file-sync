# Mocking Patterns

## Understanding Test Doubles

Test doubles are objects that stand in for real dependencies during testing. There are five main types:

```
Test Double Types:
├── Dummy    - Passed but never used
├── Stub     - Returns canned answers
├── Spy      - Records information about calls
├── Mock     - Verifies behavior expectations
└── Fake     - Working implementation (simplified)
```

## Mock vs Stub vs Fake

### Mock
**Purpose**: Verify behavior and interactions
**When**: You care about HOW a method was called

```python
# Python example - Mock
from unittest.mock import Mock

def test_order_service_sends_confirmation_email():
    # Given
    email_service = Mock()
    order_service = OrderService(email_service)
    order = Order(customer_email="user@example.com")

    # When
    order_service.place_order(order)

    # Then - Verify the interaction happened
    email_service.send_confirmation.assert_called_once_with(
        to="user@example.com",
        subject="Order Confirmation"
    )
```

```javascript
// JavaScript example - Mock
const emailService = {
  send: jest.fn()
};

test('order service sends confirmation email', () => {
  // Given
  const orderService = new OrderService(emailService);

  // When
  orderService.placeOrder({ email: 'user@example.com' });

  // Then - Verify the interaction
  expect(emailService.send).toHaveBeenCalledWith({
    to: 'user@example.com',
    subject: 'Order Confirmation'
  });
});
```

### Stub
**Purpose**: Provide predetermined responses
**When**: You care about WHAT the method returns, not how it's called

```python
# Python example - Stub
def test_order_total_calculation_with_discount():
    # Given - Stub returns a fixed discount
    discount_service = Mock()
    discount_service.get_discount.return_value = 0.20  # Stub behavior

    order_service = OrderService(discount_service)
    order = Order(subtotal=100)

    # When
    total = order_service.calculate_total(order)

    # Then - Focus on the calculation result
    assert total == 80.0  # 100 - 20% = 80
```

```java
// Java example - Stub
@Test
void shouldCalculateDiscountedTotal() {
    // Given - Stub returns a fixed value
    DiscountService discountService = mock(DiscountService.class);
    when(discountService.getDiscount(any())).thenReturn(0.20);

    OrderService orderService = new OrderService(discountService);
    Order order = new Order(100.0);

    // When
    double total = orderService.calculateTotal(order);

    // Then - Verify calculation
    assertEquals(80.0, total, 0.01);
}
```

### Fake
**Purpose**: Simplified working implementation
**When**: Need realistic behavior without external dependencies

```python
# Python example - Fake in-memory database
class FakeUserRepository:
    def __init__(self):
        self.users = {}
        self.next_id = 1

    def save(self, user):
        user.id = self.next_id
        self.users[self.next_id] = user
        self.next_id += 1
        return user

    def find_by_id(self, user_id):
        return self.users.get(user_id)

def test_user_service_creates_and_retrieves_user():
    # Given
    fake_repo = FakeUserRepository()
    user_service = UserService(fake_repo)

    # When
    created_user = user_service.create_user("John", "john@example.com")
    retrieved_user = user_service.get_user(created_user.id)

    # Then
    assert retrieved_user.name == "John"
    assert retrieved_user.email == "john@example.com"
```

```javascript
// JavaScript example - Fake HTTP client
class FakeHttpClient {
  constructor() {
    this.responses = new Map();
  }

  setResponse(url, data) {
    this.responses.set(url, data);
  }

  async get(url) {
    if (!this.responses.has(url)) {
      throw new Error('Not found');
    }
    return { data: this.responses.get(url) };
  }
}

test('API service fetches user data', async () => {
  // Given
  const fakeHttp = new FakeHttpClient();
  fakeHttp.setResponse('/users/1', { id: 1, name: 'John' });
  const apiService = new ApiService(fakeHttp);

  // When
  const user = await apiService.getUser(1);

  // Then
  expect(user.name).toBe('John');
});
```

## Common Mocking Patterns

### Pattern 1: Method Return Values

```python
# Python - Return different values for different calls
mock_service = Mock()
mock_service.get_price.side_effect = [10.0, 15.0, 20.0]

assert mock_service.get_price() == 10.0
assert mock_service.get_price() == 15.0
assert mock_service.get_price() == 20.0
```

```javascript
// JavaScript - Sequential return values
const mockService = {
  getPrice: jest.fn()
    .mockReturnValueOnce(10.0)
    .mockReturnValueOnce(15.0)
    .mockReturnValueOnce(20.0)
};
```

### Pattern 2: Conditional Return Values

```python
# Python - Return based on argument
def price_lookup(product_id):
    prices = {1: 10.0, 2: 15.0, 3: 20.0}
    return prices.get(product_id, 0.0)

mock_service = Mock()
mock_service.get_price.side_effect = price_lookup
```

```javascript
// JavaScript - Conditional mocking
mockService.getPrice.mockImplementation((productId) => {
  const prices = { 1: 10.0, 2: 15.0, 3: 20.0 };
  return prices[productId] || 0.0;
});
```

### Pattern 3: Exception Throwing

```python
# Python - Mock throws exception
mock_service = Mock()
mock_service.fetch_data.side_effect = ConnectionError("Network unavailable")

with pytest.raises(ConnectionError):
    mock_service.fetch_data()
```

```java
// Java - Mock throws exception
when(service.fetchData()).thenThrow(new ConnectionException("Network unavailable"));

assertThrows(ConnectionException.class, () -> {
    service.fetchData();
});
```

### Pattern 4: Async/Promise Mocking

```javascript
// JavaScript - Mock resolved promise
mockService.fetchUser.mockResolvedValue({ id: 1, name: 'John' });

await expect(mockService.fetchUser(1)).resolves.toEqual({
  id: 1,
  name: 'John'
});

// Mock rejected promise
mockService.fetchUser.mockRejectedValue(new Error('Not found'));

await expect(mockService.fetchUser(999)).rejects.toThrow('Not found');
```

```python
# Python - Mock async function
import asyncio
from unittest.mock import AsyncMock

async def test_async_fetch():
    mock_service = AsyncMock()
    mock_service.fetch_user.return_value = {"id": 1, "name": "John"}

    result = await mock_service.fetch_user(1)

    assert result["name"] == "John"
```

### Pattern 5: Spy Pattern (Partial Mocking)

```python
# Python - Spy on real object
from unittest.mock import patch

def test_authentication_service_logs_attempts():
    auth_service = AuthenticationService()

    with patch.object(auth_service, 'log') as mock_log:
        auth_service.authenticate('user', 'pass')

        # Verify logging happened
        mock_log.assert_called_with('Authentication attempt for user: user')
```

```javascript
// JavaScript - Spy on real method
const authService = new AuthenticationService();
const logSpy = jest.spyOn(authService, 'log');

authService.authenticate('user', 'pass');

expect(logSpy).toHaveBeenCalledWith('Authentication attempt for user: user');

logSpy.mockRestore(); // Clean up
```

## Partial Mocking

Sometimes you want to mock only specific methods while keeping others real.

```python
# Python - Partial mock
from unittest.mock import patch, MagicMock

class PaymentProcessor:
    def charge(self, amount):
        card = self.get_card()
        return self.process_payment(card, amount)

    def get_card(self):
        # Real implementation
        return "real card"

    def process_payment(self, card, amount):
        # Would hit external payment gateway
        pass

def test_payment_processor_charges_correct_amount():
    processor = PaymentProcessor()

    # Mock only process_payment, keep get_card real
    with patch.object(processor, 'process_payment', return_value=True) as mock_process:
        result = processor.charge(100.0)

        # get_card() was called for real
        mock_process.assert_called_once_with("real card", 100.0)
        assert result is True
```

```javascript
// JavaScript - Partial mock
class PaymentProcessor {
  charge(amount) {
    const card = this.getCard();
    return this.processPayment(card, amount);
  }

  getCard() {
    return 'real card';
  }

  processPayment(card, amount) {
    // Would hit external payment gateway
  }
}

test('payment processor charges correct amount', () => {
  const processor = new PaymentProcessor();

  // Mock only processPayment
  processor.processPayment = jest.fn().mockReturnValue(true);

  const result = processor.charge(100.0);

  expect(processor.processPayment).toHaveBeenCalledWith('real card', 100.0);
  expect(result).toBe(true);
});
```

## Constructor Mocking

Mocking objects created within the code under test.

```python
# Python - Mock constructor
from unittest.mock import patch, MagicMock

def test_service_creates_and_uses_logger():
    with patch('myapp.services.Logger') as MockLogger:
        mock_logger_instance = MagicMock()
        MockLogger.return_value = mock_logger_instance

        # Code that creates Logger internally
        service = MyService()  # Internally does: self.logger = Logger()
        service.do_something()

        # Verify logger was used
        mock_logger_instance.log.assert_called()
```

```java
// Java - Use dependency injection instead
// Rather than mocking constructors, prefer constructor injection

// BAD - Hard to test
class OrderService {
    private final EmailService emailService = new EmailService();
}

// GOOD - Easy to test
class OrderService {
    private final EmailService emailService;

    public OrderService(EmailService emailService) {
        this.emailService = emailService;
    }
}
```

## Argument Matchers

Flexible matching for mock verification.

```python
# Python - Argument matchers
from unittest.mock import ANY, call

mock_service.send_email(
    to="user@example.com",
    subject="Hello",
    body="Welcome"
)

# Match any value for body
mock_service.send_email.assert_called_with(
    to="user@example.com",
    subject="Hello",
    body=ANY
)
```

```java
// Java - Mockito matchers
import static org.mockito.ArgumentMatchers.*;

// Match any string
verify(emailService).send(anyString(), eq("Subject"), any());

// Match with custom matcher
verify(emailService).send(
    argThat(email -> email.contains("@")),
    eq("Subject"),
    any()
);
```

```javascript
// JavaScript - Jest matchers
expect(emailService.send).toHaveBeenCalledWith(
  expect.stringContaining('@'),
  'Subject',
  expect.any(Object)
);
```

## Mock Best Practices

### 1. Don't Mock What You Don't Own
```python
# BAD - Mocking third-party library
mock_requests = Mock()
mock_requests.get.return_value = response

# GOOD - Wrap in your own abstraction
class HttpClient:
    def get(self, url):
        return requests.get(url)

# Test with mock of your abstraction
mock_http = Mock()
mock_http.get.return_value = response
```

### 2. Keep Mocks Simple
```javascript
// BAD - Complex mock setup
const mockUser = {
  id: 1,
  name: 'John',
  address: {
    street: '123 Main',
    city: 'NYC',
    state: 'NY',
    zip: '10001'
  },
  preferences: { /* ... */ },
  // 20+ more fields
};

// GOOD - Minimal mock data
const mockUser = {
  id: 1,
  name: 'John'
};
```

### 3. Verify Meaningful Interactions
```python
# BAD - Over-verification
mock_logger.debug.assert_called()
mock_logger.info.assert_called()
mock_logger.trace.assert_called()

# GOOD - Verify what matters
mock_payment.charge.assert_called_once_with(amount=100.0)
```

### 4. Use Test Data Builders
```java
// Test data builder pattern
class UserBuilder {
    private String name = "Test User";
    private String email = "test@example.com";

    public UserBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public UserBuilder withEmail(String email) {
        this.email = email;
        return this;
    }

    public User build() {
        return new User(name, email);
    }
}

// Usage
User user = new UserBuilder()
    .withName("John")
    .build();
```

## Common Mocking Anti-Patterns

### The Mockery (Over-Mocking)
```python
# BAD - Everything is mocked
def test_user_service():
    mock_user = Mock()
    mock_user.name = "John"
    mock_service = Mock()
    mock_service.get_user.return_value = mock_user

    # Testing mock interactions, not real behavior
    result = mock_service.get_user()
    assert result.name == "John"  # This tests nothing!
```

### Fragile Mocks
```javascript
// BAD - Mock breaks on implementation changes
mock.getData.mockReturnValue({ /* specific structure */ });

// GOOD - Mock only what's needed
mock.getData.mockReturnValue({ id: 1 }); // Only ID is used
```

### Mock Leakage
```python
# BAD - Mock affects other tests
global_mock = Mock()

def test_one():
    global_mock.method.return_value = 1

def test_two():
    # Affected by test_one's setup!
    global_mock.method()

# GOOD - Isolate mocks per test
@pytest.fixture
def my_mock():
    return Mock()

def test_one(my_mock):
    my_mock.method.return_value = 1
```

## When NOT to Mock

### Don't Mock Simple Objects
```python
# BAD
mock_user = Mock()
mock_user.name = "John"
mock_user.email = "john@example.com"

# GOOD - Use real object
user = User(name="John", email="john@example.com")
```

### Don't Mock Value Objects
```javascript
// BAD
const mockDate = jest.fn().mockReturnValue('2024-01-01');

// GOOD - Use real date
const date = new Date('2024-01-01');
```

### Don't Mock Everything
```java
// BAD - Mocking the entire chain
when(mock.getA().getB().getC()).thenReturn(value);

// GOOD - Refactor to make testable
C c = subject.getC();
assertEquals(expectedValue, c.getValue());
```

## Mocking External Services

### HTTP APIs
```python
# Using responses library for HTTP mocking
import responses

@responses.activate
def test_api_client_fetches_user():
    responses.add(
        responses.GET,
        'https://api.example.com/users/1',
        json={'id': 1, 'name': 'John'},
        status=200
    )

    client = ApiClient()
    user = client.get_user(1)

    assert user['name'] == 'John'
```

### Database
```javascript
// Use in-memory database or test containers
const db = await createTestDatabase();
await db.users.insert({ name: 'John' });

const user = await userService.findByName('John');

expect(user.name).toBe('John');

await db.close();
```

### File System
```python
# Use temporary directories
import tempfile
import os

def test_file_processor():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test file
        test_file = os.path.join(tmpdir, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test data')

        # Test
        result = process_file(test_file)

        assert result == expected_output
```
