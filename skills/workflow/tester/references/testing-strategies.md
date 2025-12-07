# Testing Strategies

## The Test Pyramid

The test pyramid is a fundamental concept in software testing that guides the distribution of different types of tests.

```
        /\
       /  \      E2E Tests (Few)
      /----\     - Slow, expensive
     /      \    - Full system integration
    /--------\   - Real user workflows
   / Integration\
  /--------------\
 /   Unit Tests   \
/------------------\
```

### Test Pyramid Layers

#### Unit Tests (Base - 70%)
**What**: Test individual functions, methods, or classes in isolation
**Why**: Fast, reliable, easy to debug
**When**: For all business logic, algorithms, calculations

**Characteristics**:
- Execute in milliseconds
- No external dependencies (use mocks)
- High code coverage
- Easy to write and maintain

**Example Scenarios**:
- Validate calculation logic
- Test string parsing
- Verify data transformations
- Check validation rules

#### Integration Tests (Middle - 20%)
**What**: Test multiple components working together
**Why**: Verify component interactions and data flow
**When**: For critical integration points

**Characteristics**:
- Execute in seconds
- May use real databases (test instances)
- Test API contracts
- Verify component collaboration

**Example Scenarios**:
- Database operations (repository tests)
- API endpoint behavior
- Message queue interactions
- Service-to-service communication

#### E2E Tests (Top - 10%)
**What**: Test complete user workflows through the entire system
**Why**: Validate business scenarios from user perspective
**When**: For critical business workflows only

**Characteristics**:
- Execute in minutes
- Use real or production-like environment
- Simulate actual user behavior
- Most fragile, highest maintenance

**Example Scenarios**:
- Complete checkout flow
- User registration and login
- Document upload and processing
- Multi-step approval workflows

## When to Use Each Test Type

### Use Unit Tests When:
- Testing pure functions without side effects
- Validating business logic
- Testing edge cases and boundary values
- Ensuring algorithm correctness
- Rapid feedback is critical

**Example**:
```python
# Pure calculation - perfect for unit test
def calculate_shipping_cost(weight, distance, express):
    base_cost = weight * 0.5
    distance_cost = distance * 0.1
    return base_cost + distance_cost * (2 if express else 1)
```

### Use Integration Tests When:
- Testing database queries and transactions
- Verifying API contract compliance
- Testing component interactions
- Validating configuration
- Testing middleware/interceptors

**Example**:
```python
# Database interaction - needs integration test
def test_user_repository_creates_and_retrieves_user(db_session):
    # Given
    user = User(email="test@example.com", name="Test User")

    # When
    saved_user = user_repository.save(user, db_session)
    retrieved_user = user_repository.find_by_email("test@example.com", db_session)

    # Then
    assert retrieved_user.id == saved_user.id
    assert retrieved_user.email == "test@example.com"
```

### Use E2E Tests When:
- Testing critical business workflows
- Validating user journeys
- Smoke testing after deployment
- Testing cross-system integrations
- Compliance requirements demand it

**Example**:
```javascript
// Critical checkout flow - E2E test
test('user completes purchase successfully', async ({ page }) => {
  await page.goto('/products');
  await page.click('[data-testid="product-1"]');
  await page.click('[data-testid="add-to-cart"]');
  await page.click('[data-testid="checkout"]');
  await fillPaymentDetails(page);
  await page.click('[data-testid="submit-order"]');

  await expect(page.locator('.order-confirmation')).toBeVisible();
});
```

## Testing Strategies by Context

### API/Backend Services
```
Unit Tests (75%):
- Business logic
- Validation rules
- Data transformations
- Utility functions

Integration Tests (20%):
- Database operations
- External API calls
- Message queue interactions
- Caching behavior

E2E Tests (5%):
- Critical API workflows
- Authentication flows
- Multi-step transactions
```

### Frontend Applications
```
Unit Tests (60%):
- Component logic
- Utility functions
- State management
- Form validation

Integration Tests (30%):
- Component interactions
- API integration
- Routing behavior
- State updates

E2E Tests (10%):
- User workflows
- Cross-page navigation
- Form submissions
- Critical features
```

### Microservices
```
Unit Tests (70%):
- Service logic
- Domain models
- Validation
- Transformations

Integration Tests (25%):
- Database operations
- Message handling
- Service dependencies
- Contract testing

E2E Tests (5%):
- Business workflows
- Cross-service scenarios
- End-to-end transactions
```

## Test Coverage Guidelines

### Minimum Coverage Targets
- **Unit Tests**: 80% line coverage, 75% branch coverage
- **Integration Tests**: Cover all critical integration points
- **E2E Tests**: Cover top 5-10 user workflows

### What to Cover 100%
- Public APIs
- Business-critical logic
- Security-related code
- Payment processing
- Data validation

### What Can Have Lower Coverage
- UI layout code
- Simple getters/setters
- Configuration files
- Generated code
- Deprecated code

## Testing Strategies for Different Scenarios

### Testing Time-Dependent Code
```python
# Use dependency injection for clock
def process_order(order, clock=None):
    clock = clock or datetime.now
    order.processed_at = clock()
    return order

# Test with injected clock
def test_process_order_sets_timestamp():
    fixed_time = datetime(2024, 1, 1, 12, 0, 0)
    order = process_order(Order(), clock=lambda: fixed_time)
    assert order.processed_at == fixed_time
```

### Testing Random Behavior
```javascript
// Use seeded random or dependency injection
function shuffleArray(array, random = Math.random) {
  const result = [...array];
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

// Test with controlled random
test('shuffleArray uses random function', () => {
  const mockRandom = jest.fn()
    .mockReturnValueOnce(0.5)
    .mockReturnValueOnce(0.2);

  const result = shuffleArray([1, 2, 3], mockRandom);

  expect(mockRandom).toHaveBeenCalled();
});
```

### Testing Async Code
```javascript
// Promise-based
test('async function returns expected value', async () => {
  const result = await fetchUser(1);
  expect(result.name).toBe('John');
});

// Callback-based (use promisification)
test('callback function succeeds', (done) => {
  fetchUser(1, (error, user) => {
    expect(error).toBeNull();
    expect(user.name).toBe('John');
    done();
  });
});
```

### Testing Error Conditions
```python
# Test expected errors
def test_divide_by_zero_raises_error():
    with pytest.raises(ZeroDivisionError):
        divide(10, 0)

# Test error messages
def test_invalid_email_raises_descriptive_error():
    with pytest.raises(ValidationError, match="Invalid email format"):
        validate_email("not-an-email")
```

### Testing Side Effects
```java
@Test
void shouldSendEmailWhenOrderIsPlaced() {
    // Given
    EmailService emailService = mock(EmailService.class);
    OrderService orderService = new OrderService(emailService);
    Order order = new Order("user@example.com");

    // When
    orderService.placeOrder(order);

    // Then
    verify(emailService).sendEmail(
        eq("user@example.com"),
        eq("Order Confirmation"),
        contains("Your order has been placed")
    );
}
```

## Testing Anti-Patterns to Avoid

### The Liar
Tests that pass but don't actually verify anything
```python
# BAD
def test_calculate_total():
    result = calculate_total([1, 2, 3])
    assert result  # Just checks truthy, not actual value

# GOOD
def test_calculate_total():
    result = calculate_total([1, 2, 3])
    assert result == 6
```

### The Mockery
Over-mocking that tests nothing real
```javascript
// BAD - testing mock interactions, not real behavior
test('processes order', () => {
  const mockOrder = { process: jest.fn() };
  processOrder(mockOrder);
  expect(mockOrder.process).toHaveBeenCalled();
});

// GOOD - testing actual behavior
test('processes order updates status', () => {
  const order = new Order({ status: 'pending' });
  processOrder(order);
  expect(order.status).toBe('completed');
});
```

### The Slow Poke
Tests that take too long to run
```python
# BAD
def test_user_creation():
    time.sleep(5)  # Arbitrary delay
    user = create_user()
    assert user.id

# GOOD
def test_user_creation():
    user = create_user()
    assert user.id
```

### The Giant
Single test that verifies too much
```java
// BAD
@Test
void testEverything() {
    // 100+ lines testing multiple behaviors
}

// GOOD
@Test
void shouldCreateUser() { /* ... */ }

@Test
void shouldValidateEmail() { /* ... */ }

@Test
void shouldHashPassword() { /* ... */ }
```

## Test Data Management

### Test Data Principles
1. **Minimal**: Use the smallest dataset that proves the point
2. **Explicit**: Make test data obvious and readable
3. **Fresh**: Create new test data for each test
4. **Isolated**: Don't share mutable test data between tests

### Example
```python
# BAD - Shared mutable state
SHARED_USER = User(email="test@example.com")

def test_user_update():
    SHARED_USER.name = "Updated"  # Affects other tests!

# GOOD - Fresh data per test
@pytest.fixture
def user():
    return User(email="test@example.com")

def test_user_update(user):
    user.name = "Updated"
    assert user.name == "Updated"
```

## Continuous Testing

### Test Execution Frequency
- **Unit Tests**: On every file save (watch mode)
- **Integration Tests**: On every commit (pre-commit hook)
- **E2E Tests**: On every pull request

### CI/CD Pipeline
```yaml
# Example test stages
stages:
  - unit-tests      # Fast, runs first
  - integration     # Medium speed
  - e2e             # Slow, runs last
  - coverage        # Generate reports
```

## Test Maintenance

### Keeping Tests Maintainable
1. **Refactor tests** when refactoring code
2. **Delete tests** for removed features
3. **Update tests** when requirements change
4. **Review test coverage** regularly
5. **Fix flaky tests** immediately

### Signs Tests Need Refactoring
- Tests break on unrelated changes
- Duplicate test setup code
- Unclear test failure messages
- Tests take too long to run
- Brittle assertions (e.g., exact string matching)
