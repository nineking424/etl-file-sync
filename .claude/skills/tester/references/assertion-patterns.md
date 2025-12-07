# Assertion Patterns

## Overview

Assertions are the heart of tests - they verify that code behaves as expected. This guide covers assertion types, patterns, and best practices across different testing frameworks.

## Basic Assertion Types

### Equality Assertions

**Python (pytest)**
```python
# Exact equality
assert result == expected
assert user.name == "John"

# Not equal
assert result != unexpected
assert status != "failed"

# Deep equality for objects
assert actual_dict == {"id": 1, "name": "John"}
assert actual_list == [1, 2, 3]
```

**JavaScript (Jest)**
```javascript
// Exact equality
expect(result).toBe(expected);  // For primitives
expect(user).toEqual({ id: 1, name: 'John' });  // For objects/arrays

// Not equal
expect(result).not.toBe(unexpected);

// Reference equality
expect(objectA).toBe(objectB);  // Same reference
expect(objectA).toEqual(objectB);  // Same values
```

**Java (JUnit)**
```java
// Equality
assertEquals(expected, actual);
assertEquals("John", user.getName());

// Not equal
assertNotEquals(unexpected, actual);

// Object equality
assertEquals(expectedUser, actualUser);

// Array equality
assertArrayEquals(expectedArray, actualArray);
```

### Boolean Assertions

**Python**
```python
# Boolean values
assert is_valid
assert not is_invalid
assert user.is_active is True
assert user.is_deleted is False

# Truthy/Falsy
assert value  # Truthy
assert not value  # Falsy
```

**JavaScript**
```javascript
// Boolean values
expect(isValid).toBe(true);
expect(isInvalid).toBe(false);

// Truthy/Falsy
expect(value).toBeTruthy();
expect(value).toBeFalsy();

// Null/Undefined
expect(value).toBeNull();
expect(value).toBeUndefined();
expect(value).toBeDefined();
```

**Java**
```java
// Boolean values
assertTrue(isValid);
assertFalse(isInvalid);
assertTrue(user.isActive());

// Null checks
assertNull(value);
assertNotNull(value);
```

### Numeric Assertions

**Python**
```python
# Exact comparison
assert count == 5
assert price == 19.99

# Comparison
assert score > 0
assert age >= 18
assert temperature < 100

# Floating point comparison
assert abs(result - 3.14159) < 0.0001  # Manual delta
pytest.approx(3.14159, abs=0.0001)  # pytest helper
```

**JavaScript**
```javascript
// Exact comparison
expect(count).toBe(5);
expect(price).toBe(19.99);

// Comparison
expect(score).toBeGreaterThan(0);
expect(age).toBeGreaterThanOrEqual(18);
expect(temperature).toBeLessThan(100);

// Floating point
expect(result).toBeCloseTo(3.14159, 4);  // 4 decimal places
```

**Java**
```java
// Exact comparison
assertEquals(5, count);
assertEquals(19.99, price, 0.0);

// Comparison
assertTrue(score > 0);
assertTrue(age >= 18);

// Floating point comparison
assertEquals(3.14159, result, 0.0001);  // Delta tolerance
```

### String Assertions

**Python**
```python
# Exact match
assert message == "Hello, World!"

# Contains
assert "error" in log_message
assert "success" not in error_message

# Pattern matching
import re
assert re.match(r"\d{3}-\d{4}", phone)

# Case insensitive
assert message.lower() == "hello"

# Empty/whitespace
assert text == ""
assert text.strip() == ""
```

**JavaScript**
```javascript
// Exact match
expect(message).toBe('Hello, World!');

// Contains
expect(logMessage).toContain('error');
expect(errorMessage).not.toContain('success');

// Pattern matching
expect(phone).toMatch(/\d{3}-\d{4}/);

// Case insensitive
expect(message.toLowerCase()).toBe('hello');

// Empty/whitespace
expect(text).toBe('');
expect(text.trim()).toBe('');

// String length
expect(text).toHaveLength(10);
```

**Java**
```java
// Exact match
assertEquals("Hello, World!", message);

// Contains (using Hamcrest)
assertThat(logMessage, containsString("error"));
assertThat(errorMessage, not(containsString("success")));

// Pattern matching
assertTrue(phone.matches("\\d{3}-\\d{4}"));

// Case insensitive
assertEquals("hello", message.toLowerCase());

// Empty/whitespace
assertTrue(text.isEmpty());
assertTrue(text.trim().isEmpty());
```

### Collection Assertions

**Python**
```python
# Size
assert len(items) == 3
assert len(empty_list) == 0

# Contains
assert "apple" in fruits
assert "banana" not in fruits

# All/Any
assert all(x > 0 for x in numbers)
assert any(x % 2 == 0 for x in numbers)

# Subset/Superset
assert {1, 2}.issubset({1, 2, 3})
assert {1, 2, 3}.issuperset({1, 2})

# Empty
assert not items  # Empty
assert items  # Not empty
```

**JavaScript**
```javascript
// Size
expect(items).toHaveLength(3);
expect(emptyList).toHaveLength(0);

// Contains (array)
expect(fruits).toContain('apple');
expect(fruits).not.toContain('banana');

// Contains (object)
expect(user).toHaveProperty('name');
expect(user).toHaveProperty('email', 'john@example.com');

// Every/Some
expect(numbers.every(x => x > 0)).toBe(true);
expect(numbers.some(x => x % 2 === 0)).toBe(true);

// Array matching
expect(items).toEqual(['a', 'b', 'c']);
expect(items).toEqual(expect.arrayContaining(['a', 'b']));
```

**Java**
```java
// Size
assertEquals(3, items.size());
assertTrue(emptyList.isEmpty());

// Contains
assertTrue(fruits.contains("apple"));
assertFalse(fruits.contains("banana"));

// Using Hamcrest
assertThat(fruits, hasItem("apple"));
assertThat(fruits, not(hasItem("banana")));
assertThat(numbers, everyItem(greaterThan(0)));

// Collection equality
assertEquals(Arrays.asList("a", "b", "c"), items);
```

## Exception Assertions

**Python**
```python
import pytest

# Assert exception is raised
def test_divide_by_zero():
    with pytest.raises(ZeroDivisionError):
        divide(10, 0)

# Assert exception message
def test_invalid_email():
    with pytest.raises(ValueError, match="Invalid email"):
        validate_email("not-an-email")

# Assert exception type and inspect
def test_validation_error():
    with pytest.raises(ValidationError) as exc_info:
        validate_user(invalid_user)

    assert "email" in str(exc_info.value)
    assert exc_info.value.field == "email"
```

**JavaScript**
```javascript
// Assert function throws
test('divide by zero throws error', () => {
  expect(() => divide(10, 0)).toThrow();
  expect(() => divide(10, 0)).toThrow(ZeroDivisionError);
  expect(() => divide(10, 0)).toThrow('Cannot divide by zero');
});

// Async errors
test('async function throws error', async () => {
  await expect(fetchInvalidUser()).rejects.toThrow('User not found');
});
```

**Java**
```java
// Assert exception is thrown
@Test
void divideByZeroThrowsException() {
    assertThrows(ArithmeticException.class, () -> {
        divide(10, 0);
    });
}

// Assert exception message
@Test
void invalidEmailThrowsException() {
    Exception exception = assertThrows(ValidationException.class, () -> {
        validateEmail("not-an-email");
    });

    assertTrue(exception.getMessage().contains("Invalid email"));
}
```

## Custom Matchers

### Python Custom Assertions
```python
# Custom assertion helper
def assert_valid_email(email):
    """Assert that email is valid"""
    assert "@" in email, f"Invalid email: {email}"
    assert "." in email.split("@")[1], f"Invalid domain: {email}"

def assert_between(value, min_val, max_val):
    """Assert value is within range"""
    assert min_val <= value <= max_val, \
        f"{value} is not between {min_val} and {max_val}"

# Usage
def test_user_email():
    user = create_user()
    assert_valid_email(user.email)

def test_age():
    user = create_user()
    assert_between(user.age, 0, 120)
```

### JavaScript Custom Matchers
```javascript
// Extend Jest matchers
expect.extend({
  toBeValidEmail(received) {
    const pass = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(received);
    return {
      pass,
      message: () => `expected ${received} to be a valid email`
    };
  },

  toBeBetween(received, min, max) {
    const pass = received >= min && received <= max;
    return {
      pass,
      message: () => `expected ${received} to be between ${min} and ${max}`
    };
  }
});

// Usage
test('user has valid email', () => {
  expect(user.email).toBeValidEmail();
});

test('age is valid', () => {
  expect(user.age).toBeBetween(0, 120);
});
```

### Java Custom Matchers (Hamcrest)
```java
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

// Custom matcher
public class IsValidEmail extends TypeSafeMatcher<String> {
    @Override
    protected boolean matchesSafely(String email) {
        return email.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a valid email address");
    }

    public static Matcher<String> validEmail() {
        return new IsValidEmail();
    }
}

// Usage
@Test
void userHasValidEmail() {
    assertThat(user.getEmail(), validEmail());
}
```

## Assertion Patterns

### Pattern 1: Single Concept Per Test

```python
# BAD - Testing multiple concepts
def test_user_creation():
    user = create_user("John", "john@example.com")
    assert user.name == "John"
    assert user.email == "john@example.com"
    assert user.is_active is True
    assert user.created_at is not None
    assert len(user.permissions) == 0

# GOOD - One concept per test
def test_user_creation_sets_name():
    user = create_user("John", "john@example.com")
    assert user.name == "John"

def test_user_creation_sets_email():
    user = create_user("John", "john@example.com")
    assert user.email == "john@example.com"

def test_user_creation_activates_user():
    user = create_user("John", "john@example.com")
    assert user.is_active is True
```

### Pattern 2: Descriptive Assertion Messages

```python
# GOOD - Clear failure messages
assert user.age >= 18, f"User {user.name} is underage: {user.age}"
assert balance > 0, f"Insufficient funds: ${balance}"

# Python pytest provides good default messages
assert user.is_active  # AssertionError: assert False
```

```javascript
// JavaScript - Custom messages
expect(user.age).toBeGreaterThanOrEqual(18);  // Built-in message is good

// Custom message for complex assertions
if (user.age < 18) {
  throw new Error(`User ${user.name} is underage: ${user.age}`);
}
```

```java
// Java - Assertion messages
assertTrue(user.getAge() >= 18,
    () -> "User " + user.getName() + " is underage: " + user.getAge());
```

### Pattern 3: Exact vs Flexible Assertions

```python
# Too exact - brittle test
assert response == {
    "id": 1,
    "name": "John",
    "email": "john@example.com",
    "created_at": "2024-01-01T00:00:00Z",  # Will break!
    "updated_at": "2024-01-01T00:00:00Z"   # Will break!
}

# Flexible - test what matters
assert response["name"] == "John"
assert response["email"] == "john@example.com"
assert "created_at" in response
```

### Pattern 4: State vs Behavior Verification

```python
# State verification - check resulting state
def test_shopping_cart_adds_item():
    cart = ShoppingCart()
    item = Item("book", 29.99)

    cart.add(item)

    # Verify state
    assert len(cart.items) == 1
    assert cart.items[0] == item
    assert cart.total == 29.99

# Behavior verification - check interactions
from unittest.mock import Mock

def test_order_service_notifies_customer():
    notification_service = Mock()
    order_service = OrderService(notification_service)

    order_service.place_order(order)

    # Verify behavior
    notification_service.send.assert_called_once_with(
        customer=order.customer,
        type="order_confirmation"
    )
```

### Pattern 5: Snapshot Testing

```javascript
// JavaScript - Jest snapshots
test('renders component correctly', () => {
  const component = render(<UserProfile user={user} />);

  expect(component).toMatchSnapshot();
});

// Update snapshots when UI intentionally changes
// jest --updateSnapshot
```

### Pattern 6: Table-Driven Assertions

```python
# Python - Parameterized tests
import pytest

@pytest.mark.parametrize("input,expected", [
    (0, "zero"),
    (1, "one"),
    (-1, "negative one"),
    (100, "one hundred"),
])
def test_number_to_words(input, expected):
    result = number_to_words(input)
    assert result == expected
```

```javascript
// JavaScript - Test each
test.each([
  [0, 'zero'],
  [1, 'one'],
  [-1, 'negative one'],
  [100, 'one hundred'],
])('converts %i to %s', (input, expected) => {
  expect(numberToWords(input)).toBe(expected);
});
```

```java
// Java - Parameterized tests
@ParameterizedTest
@CsvSource({
    "0, zero",
    "1, one",
    "-1, negative one",
    "100, one hundred"
})
void convertsNumberToWords(int input, String expected) {
    String result = numberToWords(input);
    assertEquals(expected, result);
}
```

## Async Assertion Patterns

### Python Async
```python
import pytest

@pytest.mark.asyncio
async def test_async_user_fetch():
    user = await fetch_user(1)

    assert user.id == 1
    assert user.name == "John"

@pytest.mark.asyncio
async def test_async_exception():
    with pytest.raises(UserNotFoundError):
        await fetch_user(999)
```

### JavaScript Async
```javascript
// Promise-based
test('fetches user', async () => {
  const user = await fetchUser(1);

  expect(user.id).toBe(1);
  expect(user.name).toBe('John');
});

// resolves/rejects
test('fetches user', () => {
  return expect(fetchUser(1)).resolves.toMatchObject({
    id: 1,
    name: 'John'
  });
});

test('fails for invalid user', () => {
  return expect(fetchUser(999)).rejects.toThrow('User not found');
});
```

## Assertion Best Practices

### 1. One Logical Assertion Per Test
```python
# GOOD - Clear test focus
def test_calculate_discount_for_premium_users():
    user = User(tier="premium")
    discount = calculate_discount(user, 100)
    assert discount == 20.0

def test_calculate_discount_for_regular_users():
    user = User(tier="regular")
    discount = calculate_discount(user, 100)
    assert discount == 10.0
```

### 2. Avoid Conditional Assertions
```python
# BAD
def test_user_status():
    user = get_user()
    if user.is_premium:
        assert user.discount > 0
    else:
        assert user.discount == 0

# GOOD - Separate tests
def test_premium_user_has_discount():
    user = create_premium_user()
    assert user.discount > 0

def test_regular_user_has_no_discount():
    user = create_regular_user()
    assert user.discount == 0
```

### 3. Assert on Meaningful Values
```python
# BAD - Magic numbers
assert result == 42

# GOOD - Named constants
EXPECTED_DISCOUNT_PERCENT = 20
assert result == EXPECTED_DISCOUNT_PERCENT
```

### 4. Test Boundaries
```python
def test_age_validation():
    # Test boundary values
    assert is_valid_age(0) is True    # Min valid
    assert is_valid_age(-1) is False  # Below min
    assert is_valid_age(120) is True  # Max valid
    assert is_valid_age(121) is False # Above max
```

### 5. Verify Complete State When Necessary
```python
def test_user_creation_complete():
    user = create_user("John", "john@example.com")

    # Verify all important fields
    assert user.name == "John"
    assert user.email == "john@example.com"
    assert user.is_active is True
    assert user.role == "user"
    assert user.created_at is not None
```

## Anti-Patterns to Avoid

### No Assertions
```python
# BAD - No verification
def test_user_creation():
    user = create_user("John", "john@example.com")
    # Missing assertion!

# GOOD
def test_user_creation():
    user = create_user("John", "john@example.com")
    assert user.name == "John"
```

### Assertion Roulette
```python
# BAD - Unclear which assertion failed
assert a == b
assert c == d
assert e == f
assert g == h

# GOOD - Descriptive messages or separate tests
assert a == b, "User ID mismatch"
assert c == d, "Email mismatch"
```

### Irrelevant Assertions
```python
# BAD - Testing framework behavior
def test_list_creation():
    items = []
    assert isinstance(items, list)  # Always true!
    assert len(items) == 0  # Obvious!

# GOOD - Test your code
def test_shopping_cart_starts_empty():
    cart = ShoppingCart()
    assert cart.item_count() == 0
```

### Over-Assertion
```python
# BAD - Testing internal implementation
def test_user_service():
    assert service._internal_cache == {}  # Private detail!
    assert service._counter == 0  # Private detail!

# GOOD - Test public behavior
def test_user_service_creates_user():
    user = service.create_user("John")
    assert service.get_user(user.id) == user
```

### Sleepy Assertions
```javascript
// BAD - Arbitrary waits
test('data loads', async () => {
  loadData();
  await sleep(1000);  // Fragile!
  expect(data).toBeDefined();
});

// GOOD - Wait for condition
test('data loads', async () => {
  await waitFor(() => {
    expect(data).toBeDefined();
  });
});
```
