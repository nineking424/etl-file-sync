# Code Review Comment Templates

Ready-to-use templates for providing clear, constructive, and actionable code review feedback. These templates help maintain consistency and professionalism in code reviews.

## Template Structure

All comments follow this structure:
```
**[Priority]** [Category]: [Brief Description]

[Detailed explanation]

Suggestion:
[Concrete recommendation with code example if applicable]

[Optional: Reference to principles/documentation]
```

## 1. Bug and Logic Errors

### Potential Null Pointer Exception
```
**Critical** Null Safety: Potential null pointer exception

The variable `user` could be null if the database query returns no results, which would cause a runtime exception at line 45.

Suggestion:
Add a null check before accessing user properties:

if (user == null) {
    throw new UserNotFoundException("User not found");
}

Or use optional chaining:
const userName = user?.name ?? 'Unknown';
```

### Off-by-One Error
```
**Major** Logic Error: Off-by-one error in loop condition

The loop condition `i <= array.length` will cause an index out of bounds error on the last iteration.

Suggestion:
Change the condition to:
for (let i = 0; i < array.length; i++) {
    // ...
}
```

### Race Condition
```
**Critical** Concurrency: Potential race condition

The `counter` variable is accessed by multiple threads without synchronization, which can lead to incorrect values.

Suggestion:
Use atomic operations or synchronization:

private final AtomicInteger counter = new AtomicInteger(0);

public void increment() {
    counter.incrementAndGet();
}
```

### Missing Error Handling
```
**Major** Error Handling: Unhandled exception

The file operation can throw `IOException`, but it's not being caught or declared, which could crash the application.

Suggestion:
Wrap in try-catch block:

try {
    String content = Files.readString(Path.of(filename));
    // process content
} catch (IOException e) {
    logger.error("Failed to read file: " + filename, e);
    throw new FileProcessingException("Cannot process file", e);
}
```

## 2. Security Issues

### SQL Injection Vulnerability
```
**Critical** Security: SQL injection vulnerability

The query is constructed using string concatenation with user input, which allows SQL injection attacks.

Suggestion:
Use parameterized queries:

// Bad
String query = "SELECT * FROM users WHERE username = '" + username + "'";

// Good
String query = "SELECT * FROM users WHERE username = ?";
PreparedStatement stmt = connection.prepareStatement(query);
stmt.setString(1, username);
```

### XSS Vulnerability
```
**Critical** Security: Cross-site scripting (XSS) vulnerability

User input is directly inserted into HTML without sanitization, allowing malicious scripts to be executed.

Suggestion:
Sanitize user input before rendering:

// Use a library like DOMPurify
const sanitizedInput = DOMPurify.sanitize(userInput);
element.innerHTML = sanitizedInput;

// Or escape HTML entities
const escapedInput = escapeHtml(userInput);
```

### Exposed Secrets
```
**Critical** Security: Hardcoded credentials

API keys and passwords should never be hardcoded in the source code.

Suggestion:
Move secrets to environment variables or a secure vault:

// Bad
const API_KEY = "sk_live_51H...";

// Good
const API_KEY = process.env.API_KEY;

Ensure the .env file is added to .gitignore.
```

### Weak Authentication
```
**Critical** Security: Weak password hashing

Passwords are stored using MD5, which is cryptographically broken and unsuitable for password storage.

Suggestion:
Use bcrypt or Argon2 for password hashing:

import bcrypt from 'bcrypt';

const saltRounds = 10;
const hashedPassword = await bcrypt.hash(password, saltRounds);
```

## 3. Performance Issues

### N+1 Query Problem
```
**Major** Performance: N+1 query problem

Loading related data in a loop causes multiple database queries. For 100 users, this results in 101 queries (1 + 100).

Suggestion:
Use eager loading to fetch all related data in a single query:

// Bad
const users = await User.findAll();
for (const user of users) {
    const posts = await Post.findAll({ where: { userId: user.id } });
}

// Good
const users = await User.findAll({
    include: [{ model: Post }]
});
```

### Inefficient Algorithm
```
**Major** Performance: Inefficient algorithm (O(n²))

The nested loop creates quadratic time complexity. For large datasets, this will be very slow.

Suggestion:
Use a hash map for O(n) complexity:

// Bad - O(n²)
for (const item1 of array1) {
    for (const item2 of array2) {
        if (item1.id === item2.id) {
            // ...
        }
    }
}

// Good - O(n)
const map = new Map(array2.map(item => [item.id, item]));
for (const item1 of array1) {
    const match = map.get(item1.id);
    if (match) {
        // ...
    }
}
```

### Memory Leak
```
**Critical** Performance: Memory leak

Event listeners are added but never removed, causing memory leaks.

Suggestion:
Remove event listeners when component unmounts:

useEffect(() => {
    const handleResize = () => { /* ... */ };
    window.addEventListener('resize', handleResize);

    // Cleanup function
    return () => {
        window.removeEventListener('resize', handleResize);
    };
}, []);
```

## 4. Code Quality

### High Complexity
```
**Major** Complexity: High cyclomatic complexity (15)

This function has too many decision points, making it difficult to understand, test, and maintain.

Suggestion:
Extract validation logic into separate functions:

function processOrder(order) {
    validateOrder(order);
    const total = calculateTotal(order);
    applyDiscounts(order, total);
    return saveOrder(order);
}

function validateOrder(order) {
    validateCustomer(order.customer);
    validateItems(order.items);
    validateShipping(order.shipping);
}

Reference: Clean Code Principles - Single Responsibility Principle
```

### Deep Nesting
```
**Major** Readability: Excessive nesting depth (6 levels)

Deep nesting makes code hard to read and understand.

Suggestion:
Use early returns and extract functions:

// Bad
function processUser(user) {
    if (user) {
        if (user.isActive) {
            if (user.hasPermission) {
                if (user.emailVerified) {
                    // deeply nested logic
                }
            }
        }
    }
}

// Good
function processUser(user) {
    if (!user) return;
    if (!user.isActive) return;
    if (!user.hasPermission) return;
    if (!user.emailVerified) return;

    // logic at root level
}
```

### Code Duplication
```
**Major** Maintainability: Duplicate code

This logic is repeated in 3 different places. Changes need to be made in multiple locations, increasing error risk.

Suggestion:
Extract common logic into a reusable function:

function calculateDiscount(amount, customerType) {
    const rates = {
        regular: 0.1,
        premium: 0.2,
        vip: 0.3
    };
    return amount * (rates[customerType] || 0);
}

Reference: DRY Principle
```

### Magic Numbers
```
**Minor** Maintainability: Magic numbers

Hardcoded numbers make the code unclear and difficult to maintain.

Suggestion:
Use named constants:

// Bad
if (user.age >= 18 && user.score > 750) {
    // ...
}

// Good
const MINIMUM_AGE = 18;
const CREDIT_SCORE_THRESHOLD = 750;

if (user.age >= MINIMUM_AGE && user.score > CREDIT_SCORE_THRESHOLD) {
    // ...
}
```

## 5. Naming and Readability

### Unclear Variable Names
```
**Minor** Naming: Unclear variable name

The variable name 'd' doesn't convey its purpose.

Suggestion:
Use descriptive names:

// Bad
const d = new Date();
const t = 86400;

// Good
const currentDate = new Date();
const SECONDS_PER_DAY = 86400;
```

### Inconsistent Naming
```
**Minor** Consistency: Inconsistent naming convention

The codebase uses both camelCase and snake_case. Pick one convention and stick to it.

Suggestion:
Follow the project's naming conventions (appears to be camelCase):

// Inconsistent
const user_name = getUserData();
const userEmail = getEmailAddress();

// Consistent
const userName = getUserData();
const userEmail = getEmailAddress();
```

### Misleading Name
```
**Major** Naming: Misleading function name

The function is named `getUsers()` but it also deletes inactive users, which is an unexpected side effect.

Suggestion:
Rename to reflect actual behavior or split into two functions:

// Option 1: Rename
function getAndCleanUsers() {
    deleteInactiveUsers();
    return getActiveUsers();
}

// Option 2: Split (preferred)
function cleanInactiveUsers() {
    deleteInactiveUsers();
}

function getActiveUsers() {
    return fetchActiveUsers();
}
```

## 6. Architecture and Design

### Violation of Single Responsibility
```
**Major** Design: Single Responsibility Principle violation

This class handles user management, email sending, and report generation. It has too many responsibilities.

Suggestion:
Split into focused classes:

class UserService {
    // User CRUD operations only
}

class EmailService {
    // Email operations only
}

class ReportService {
    // Report generation only
}

Reference: SOLID Principles - Single Responsibility
```

### Tight Coupling
```
**Major** Design: Tight coupling

The `OrderService` is tightly coupled to `MySQLDatabase`, making it difficult to test and swap implementations.

Suggestion:
Use dependency injection:

// Bad
class OrderService {
    private database = new MySQLDatabase();
}

// Good
class OrderService {
    constructor(private database: Database) {}
}

// Usage
const service = new OrderService(new MySQLDatabase());

Reference: SOLID Principles - Dependency Inversion
```

### Missing Abstraction
```
**Major** Design: Missing abstraction layer

Business logic is mixed with HTTP handling, making it difficult to test and reuse.

Suggestion:
Extract business logic into a service layer:

// Bad
app.post('/users', async (req, res) => {
    const user = req.body;
    // validation logic
    // business logic
    // database logic
    res.json(result);
});

// Good
app.post('/users', async (req, res) => {
    const user = req.body;
    const result = await userService.createUser(user);
    res.json(result);
});
```

## 7. Testing

### Missing Tests
```
**Major** Testing: No test coverage for new functionality

The new payment processing logic has no automated tests, which is risky for such critical functionality.

Suggestion:
Add unit tests covering:
- Successful payment processing
- Payment failures
- Invalid input handling
- Edge cases (zero amount, negative amount)

Example:
describe('PaymentService', () => {
    it('should process valid payment successfully', async () => {
        const result = await service.processPayment(validPayment);
        expect(result.status).toBe('success');
    });

    it('should reject invalid payment amount', async () => {
        await expect(service.processPayment({amount: -100}))
            .rejects.toThrow('Invalid amount');
    });
});
```

### Flaky Test
```
**Major** Testing: Flaky test due to timing dependency

The test depends on a setTimeout which makes it non-deterministic and may fail randomly.

Suggestion:
Use proper async/await or mock timers:

// Bad
test('debounce works', (done) => {
    debounce(fn, 100);
    setTimeout(() => {
        expect(fn).toHaveBeenCalled();
        done();
    }, 150);
});

// Good
test('debounce works', async () => {
    jest.useFakeTimers();
    debounce(fn, 100);
    jest.advanceTimersByTime(100);
    expect(fn).toHaveBeenCalled();
});
```

### Test Doing Too Much
```
**Minor** Testing: Test covers multiple scenarios

This test validates multiple behaviors, making it unclear which scenario failed if the test breaks.

Suggestion:
Split into separate test cases:

// Bad
test('user operations', () => {
    // create user
    // update user
    // delete user
});

// Good
test('should create user', () => { /* ... */ });
test('should update user', () => { /* ... */ });
test('should delete user', () => { /* ... */ });
```

## 8. Documentation

### Missing Documentation
```
**Minor** Documentation: Missing JSDoc for public API

This public method lacks documentation, making it unclear what parameters are expected and what it returns.

Suggestion:
Add JSDoc comments:

/**
 * Processes a user order and returns the confirmation.
 *
 * @param {Object} order - The order object
 * @param {string} order.userId - The user's ID
 * @param {Array<Object>} order.items - Array of order items
 * @returns {Promise<OrderConfirmation>} The order confirmation
 * @throws {ValidationError} If order data is invalid
 * @throws {PaymentError} If payment processing fails
 */
async function processOrder(order) {
    // ...
}
```

### Outdated Documentation
```
**Minor** Documentation: Documentation doesn't match implementation

The comment says the function returns a string, but it actually returns a Promise<string>.

Suggestion:
Update documentation to match current implementation:

// Update this
// @returns {string} The user's name

// To this
// @returns {Promise<string>} The user's name
```

## 9. Positive Feedback

### Well-Structured Code
```
**Praise** Great abstraction!

I really like how you've separated concerns here. The service layer is clean and the business logic is well-isolated from the HTTP handling. This makes the code very testable and maintainable.
```

### Good Error Handling
```
**Praise** Excellent error handling

The error handling here is thorough and provides clear, actionable error messages. The use of custom error types makes it easy to handle different error scenarios appropriately.
```

### Clear Naming
```
**Praise** Clear and descriptive names

The variable and function names make the code self-documenting. It's immediately clear what each piece does without needing comments.
```

## 10. Questions and Clarifications

### Seeking Clarification
```
**Question** Clarification needed

I'm not sure I understand the purpose of this check. Could you explain what scenario this is handling?

It seems like this might be related to [specific case], but I want to make sure I understand the intent before suggesting changes.
```

### Performance Question
```
**Question** Performance consideration

Have you considered the performance implications of loading all records into memory? With large datasets, this could cause memory issues.

Is there a way to process this in batches or use pagination?
```

### Alternative Approach
```
**Suggestion** Alternative approach

Have you considered using [specific pattern/library]? It might simplify this logic and provide better error handling.

I'm curious about your thoughts on this approach vs. the current implementation.
```

## 11. Style and Formatting

### Formatting Inconsistency
```
**Minor** Style: Inconsistent formatting

The indentation mixes tabs and spaces. Please use the project's formatting standard (spaces, configured in .editorconfig).

Suggestion:
Run the formatter:
npm run format

Or configure your editor to use the project's formatting rules.
```

### Import Organization
```
**Minor** Style: Disorganized imports

Imports would be more readable if grouped and sorted.

Suggestion:
Organize as:
1. External dependencies
2. Internal modules
3. Types/interfaces

// External
import React from 'react';
import axios from 'axios';

// Internal
import { UserService } from './services/UserService';
import { formatDate } from './utils/date';

// Types
import type { User } from './types';
```

## Usage Guidelines

### When to Use Templates

1. **Use templates as starting points**: Customize for specific context
2. **Combine templates**: Multiple issues may exist in one location
3. **Adjust tone**: Match your team's culture
4. **Be specific**: Add line numbers, actual code snippets
5. **Provide context**: Explain why something matters

### Best Practices

1. **Be constructive**: Focus on improvement, not criticism
2. **Be specific**: Point to exact issues and solutions
3. **Be kind**: Assume positive intent
4. **Be educational**: Explain the reasoning
5. **Be consistent**: Use similar language for similar issues
6. **Balance feedback**: Acknowledge good work too
7. **Prioritize correctly**: Critical issues first

### Customizing Templates

Feel free to adapt these templates to your:
- Team's communication style
- Project's specific needs
- Language or framework conventions
- Organization's standards

The goal is clear, actionable, and respectful feedback that helps improve code quality.
