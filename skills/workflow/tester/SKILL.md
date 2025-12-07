---
name: tester
description: Generate comprehensive test suites with automatic boundary value analysis, mocking patterns, and coverage guidance across multiple testing frameworks
---

# Tester Skill

## Overview

The Tester skill helps you generate comprehensive, maintainable test suites following industry best practices. It automatically derives edge cases, applies the Given-When-Then pattern, generates mocks/stubs, and provides coverage analysis guidance.

## Core Workflow

When asked to generate tests, follow this workflow:

### 1. Analyze the Code Under Test
- Identify the code's purpose and scope
- Extract input parameters, return types, and dependencies
- Map out execution paths and decision points
- Identify external dependencies requiring mocking

### 2. Derive Test Cases
- **Happy Path**: Normal operation with valid inputs
- **Boundary Values**: Min/max values, empty collections, null/undefined
- **Edge Cases**: Special values (0, -1, MAX_INT), empty strings, special characters
- **Error Cases**: Invalid inputs, null pointers, exceptions
- **State Transitions**: Different object states and their interactions

### 3. Select Testing Level
- **Unit Tests**: Single function/method in isolation
- **Integration Tests**: Multiple components working together
- **E2E Tests**: Full user workflows through the system

### 4. Generate Test Code
Apply the appropriate framework template and patterns:
- Use Given-When-Then structure
- Generate necessary mocks/stubs
- Include descriptive test names
- Add meaningful assertions
- Group related tests

### 5. Coverage Analysis
- Identify uncovered branches
- Suggest additional test cases
- Recommend edge cases that may have been missed

## Testing Patterns

### Unit Testing
Focus on testing individual functions/methods in isolation.

```python
# pytest example
def test_calculate_discount_for_premium_customer():
    # Given
    customer = Customer(tier="premium", purchase_amount=100)

    # When
    discount = calculate_discount(customer)

    # Then
    assert discount == 20.0
```

```javascript
// Jest example
describe('calculateDiscount', () => {
  test('should apply 20% discount for premium customers', () => {
    // Given
    const customer = { tier: 'premium', purchaseAmount: 100 };

    // When
    const discount = calculateDiscount(customer);

    // Then
    expect(discount).toBe(20.0);
  });
});
```

```java
// JUnit with Mockito
@Test
void shouldApply20PercentDiscountForPremiumCustomers() {
    // Given
    Customer customer = new Customer("premium", 100.0);

    // When
    double discount = discountCalculator.calculate(customer);

    // Then
    assertEquals(20.0, discount, 0.01);
}
```

### Integration Testing
Test multiple components working together.

```python
# pytest with fixtures
def test_order_processing_workflow(db_session, mock_payment_gateway):
    # Given
    order = Order(items=[Item("book", 29.99)])
    db_session.add(order)
    mock_payment_gateway.charge.return_value = PaymentResult(success=True)

    # When
    result = process_order(order, db_session, mock_payment_gateway)

    # Then
    assert result.status == "completed"
    assert db_session.query(Order).filter_by(id=order.id).first().status == "paid"
    mock_payment_gateway.charge.assert_called_once()
```

### E2E Testing
Test complete user workflows.

```javascript
// Playwright/Cypress example
test('user can complete purchase flow', async ({ page }) => {
  // Given
  await page.goto('/products');

  // When
  await page.click('[data-testid="add-to-cart"]');
  await page.click('[data-testid="checkout"]');
  await page.fill('[name="email"]', 'user@example.com');
  await page.fill('[name="card"]', '4242424242424242');
  await page.click('[data-testid="complete-purchase"]');

  // Then
  await expect(page.locator('[data-testid="success-message"]')).toBeVisible();
});
```

## Framework Templates

### pytest (Python)
```python
import pytest
from unittest.mock import Mock, patch

class TestClassName:
    """Test suite for ClassName"""

    @pytest.fixture
    def subject(self):
        """Create instance under test"""
        return ClassName()

    def test_method_name_with_valid_input(self, subject):
        # Given
        input_value = "valid"

        # When
        result = subject.method_name(input_value)

        # Then
        assert result == expected_value

    def test_method_name_with_null_input(self, subject):
        # Given
        input_value = None

        # When/Then
        with pytest.raises(ValueError):
            subject.method_name(input_value)

    @pytest.mark.parametrize("input,expected", [
        (0, "zero"),
        (1, "one"),
        (-1, "negative"),
        (999999, "large"),
    ])
    def test_method_name_boundary_values(self, subject, input, expected):
        # When
        result = subject.method_name(input)

        # Then
        assert result == expected
```

### Jest (JavaScript/TypeScript)
```javascript
import { jest } from '@jest/globals';

describe('ClassName', () => {
  let subject;
  let mockDependency;

  beforeEach(() => {
    mockDependency = {
      method: jest.fn()
    };
    subject = new ClassName(mockDependency);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('methodName', () => {
    test('should return expected value with valid input', () => {
      // Given
      const input = 'valid';
      mockDependency.method.mockReturnValue('mocked');

      // When
      const result = subject.methodName(input);

      // Then
      expect(result).toBe('expected');
      expect(mockDependency.method).toHaveBeenCalledWith(input);
    });

    test('should throw error with null input', () => {
      // Given
      const input = null;

      // When/Then
      expect(() => subject.methodName(input)).toThrow('Invalid input');
    });

    test.each([
      [0, 'zero'],
      [1, 'one'],
      [-1, 'negative'],
      [Number.MAX_SAFE_INTEGER, 'max'],
    ])('should handle boundary value %i returning %s', (input, expected) => {
      // When
      const result = subject.methodName(input);

      // Then
      expect(result).toBe(expected);
    });
  });
});
```

### JUnit with Mockito (Java)
```java
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("ClassName Tests")
class ClassNameTest {

    @Mock
    private Dependency mockDependency;

    @InjectMocks
    private ClassName subject;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    @DisplayName("should return expected value with valid input")
    void shouldReturnExpectedValueWithValidInput() {
        // Given
        String input = "valid";
        when(mockDependency.method(input)).thenReturn("mocked");

        // When
        String result = subject.methodName(input);

        // Then
        assertEquals("expected", result);
        verify(mockDependency).method(input);
    }

    @Test
    @DisplayName("should throw exception with null input")
    void shouldThrowExceptionWithNullInput() {
        // Given
        String input = null;

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> {
            subject.methodName(input);
        });
    }

    @ParameterizedTest
    @CsvSource({
        "0, zero",
        "1, one",
        "-1, negative",
        "999999, large"
    })
    @DisplayName("should handle boundary values")
    void shouldHandleBoundaryValues(int input, String expected) {
        // When
        String result = subject.methodName(input);

        // Then
        assertEquals(expected, result);
    }
}
```

## Boundary Value & Edge Case Auto-Derivation

For any input type, automatically consider these test cases:

### Numeric Types
- **Zero**: `0`
- **Negative**: `-1`, minimum value
- **Positive**: `1`, maximum value
- **Boundary**: `MAX_VALUE`, `MIN_VALUE`, `MAX_VALUE - 1`, `MIN_VALUE + 1`
- **Special**: `NaN`, `Infinity`, `-Infinity` (for floating point)

### Strings
- **Empty**: `""`
- **Single character**: `"a"`
- **Whitespace**: `" "`, `"\t"`, `"\n"`
- **Special characters**: Unicode, emojis, SQL injection patterns
- **Long strings**: Maximum length, length + 1
- **Null**: `null`, `undefined`

### Collections (Arrays, Lists, Sets)
- **Empty**: `[]`, `{}`
- **Single element**: `[item]`
- **Multiple elements**: `[item1, item2, item3]`
- **Maximum size**: At capacity
- **Null elements**: `[null]`, `[item, null, item]`
- **Duplicates**: `[item, item, item]`

### Booleans
- **True**: `true`
- **False**: `false`
- **Null**: `null` (in nullable contexts)

### Dates/Times
- **Past**: Yesterday, last year
- **Present**: Now
- **Future**: Tomorrow, next year
- **Epoch**: `0`, Unix epoch start
- **Boundary**: Year 2038 problem, leap years

### Objects
- **Null**: `null`
- **Empty**: `{}`
- **Partial**: Missing required fields
- **Invalid**: Wrong types for fields
- **Complete**: All fields valid

## Given-When-Then Pattern

Structure all tests using the Given-When-Then pattern for clarity:

```
// Given - Set up test preconditions
//   - Create test data
//   - Configure mocks
//   - Set up system state

// When - Execute the action being tested
//   - Call the method/function
//   - Trigger the event

// Then - Verify the outcome
//   - Assert return values
//   - Verify state changes
//   - Check mock interactions
```

## Mock/Stub Auto-Generation

### When to Mock
- External services (APIs, databases)
- File system operations
- Time-dependent code
- Random number generation
- Network calls
- Heavy dependencies

### Mock Generation Rules

**Python (unittest.mock)**
```python
from unittest.mock import Mock, patch, MagicMock

# Simple mock
mock_service = Mock()
mock_service.get_user.return_value = User(id=1, name="Test")

# Patch decorator
@patch('module.ServiceClass')
def test_function(mock_service):
    mock_service.return_value.method.return_value = "mocked"

# Context manager
with patch('module.function') as mock_func:
    mock_func.return_value = "mocked"
```

**JavaScript (Jest)**
```javascript
// Mock module
jest.mock('./service');

// Mock function
const mockFn = jest.fn().mockReturnValue('mocked');

// Mock implementation
mockFn.mockImplementation((arg) => {
  return arg * 2;
});

// Mock resolved promise
mockFn.mockResolvedValue({ data: 'mocked' });
```

**Java (Mockito)**
```java
// Mock creation
Service mockService = mock(Service.class);

// Stub method
when(mockService.getUser(1)).thenReturn(new User(1, "Test"));

// Verify interaction
verify(mockService).getUser(1);
verify(mockService, times(2)).method();
```

## Coverage Analysis Guidance

### Code Coverage Metrics
- **Line Coverage**: Aim for >80%
- **Branch Coverage**: Aim for >75%
- **Function Coverage**: Aim for 100%
- **Statement Coverage**: Aim for >80%

### Coverage Analysis Steps
1. Run tests with coverage tool
2. Identify uncovered lines/branches
3. Determine if uncovered code is:
   - Error handling paths
   - Edge cases
   - Dead code (consider removing)
4. Generate tests for critical uncovered paths
5. Document why some code remains untested (if justified)

### Coverage Tools
- **Python**: `pytest-cov`, `coverage.py`
- **JavaScript**: `jest --coverage`, `nyc`
- **Java**: `JaCoCo`, `Cobertura`

### Example Coverage Report Analysis
```
File: discount_calculator.py
Line Coverage: 85%
Branch Coverage: 70%

Uncovered lines:
- Line 45-47: Error handling for negative amounts
- Line 62: Edge case for VIP customers with zero purchases

Recommendation:
- Add test for negative purchase amounts
- Add test for VIP customer with purchaseAmount = 0
```

## Test Organization

### File Naming
- Python: `test_<module_name>.py`
- JavaScript: `<module_name>.test.js` or `<module_name>.spec.js`
- Java: `<ClassName>Test.java`

### Directory Structure
```
project/
├── src/
│   └── calculator.py
└── tests/
    ├── unit/
    │   └── test_calculator.py
    ├── integration/
    │   └── test_order_processing.py
    └── e2e/
        └── test_checkout_flow.py
```

## Best Practices

1. **Test One Thing**: Each test should verify one behavior
2. **Independent Tests**: Tests should not depend on each other
3. **Deterministic**: Tests should produce the same result every time
4. **Fast**: Unit tests should run in milliseconds
5. **Readable**: Use descriptive names and clear structure
6. **Maintainable**: Keep tests DRY but prefer clarity over brevity
7. **Isolated**: Use mocks to isolate the code under test
8. **Complete**: Test happy path, edge cases, and error conditions

## Common Anti-Patterns to Avoid

- **Testing Implementation Details**: Test behavior, not internal structure
- **Fragile Tests**: Avoid over-specific assertions
- **Test Interdependence**: Each test should run independently
- **Magic Numbers**: Use named constants for test data
- **Incomplete Assertions**: Verify all relevant outcomes
- **Ignoring Async**: Properly handle promises and async operations

## References

See the `references/` directory for detailed guides on:
- Testing strategies and the test pyramid
- Mocking patterns (Mock vs Stub vs Fake)
- Test data patterns (Object Mother, Builder, Factory)
- Assertion patterns and custom matchers
