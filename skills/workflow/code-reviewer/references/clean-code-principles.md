# Clean Code Principles

A comprehensive guide to writing clean, maintainable, and professional code based on industry-standard principles and best practices.

## SOLID Principles

The five SOLID principles are the foundation of good object-oriented design.

### S - Single Responsibility Principle (SRP)

**Definition:** A class should have only one reason to change. Each class should have a single, well-defined responsibility.

**Why It Matters:**
- Easier to understand and maintain
- Lower coupling between components
- Changes are isolated and predictable
- Easier to test

**Bad Example:**
```java
class User {
    private String name;
    private String email;

    // Multiple responsibilities: business logic, persistence, and formatting
    public void save() {
        // Database logic
        Connection conn = DriverManager.getConnection("...");
        // SQL operations
    }

    public void sendEmail(String message) {
        // Email sending logic
        SmtpClient client = new SmtpClient();
        // Send email
    }

    public String toJSON() {
        // Formatting logic
        return "{\"name\":\"" + name + "\",\"email\":\"" + email + "\"}";
    }
}
```

**Good Example:**
```java
// Single responsibility: User data model
class User {
    private String name;
    private String email;

    public String getName() { return name; }
    public String getEmail() { return email; }
}

// Single responsibility: User persistence
class UserRepository {
    public void save(User user) {
        // Database logic only
    }
}

// Single responsibility: Email notifications
class EmailService {
    public void sendEmail(User user, String message) {
        // Email logic only
    }
}

// Single responsibility: User serialization
class UserSerializer {
    public String toJSON(User user) {
        // Formatting logic only
    }
}
```

### O - Open/Closed Principle (OCP)

**Definition:** Software entities should be open for extension but closed for modification.

**Why It Matters:**
- Add new features without changing existing code
- Reduces risk of breaking existing functionality
- Promotes stability in the codebase

**Bad Example:**
```python
class DiscountCalculator:
    def calculate(self, customer_type, amount):
        if customer_type == "regular":
            return amount * 0.1
        elif customer_type == "premium":
            return amount * 0.2
        elif customer_type == "vip":
            return amount * 0.3
        # Adding new customer type requires modifying this class
```

**Good Example:**
```python
# Open for extension, closed for modification
class DiscountStrategy:
    def calculate(self, amount):
        raise NotImplementedError

class RegularDiscount(DiscountStrategy):
    def calculate(self, amount):
        return amount * 0.1

class PremiumDiscount(DiscountStrategy):
    def calculate(self, amount):
        return amount * 0.2

class VIPDiscount(DiscountStrategy):
    def calculate(self, amount):
        return amount * 0.3

# New customer types can be added without modifying existing code
class GoldDiscount(DiscountStrategy):
    def calculate(self, amount):
        return amount * 0.4
```

### L - Liskov Substitution Principle (LSP)

**Definition:** Objects of a superclass should be replaceable with objects of a subclass without breaking the application.

**Why It Matters:**
- Ensures proper inheritance relationships
- Prevents unexpected behavior from substitution
- Maintains contract consistency

**Bad Example:**
```typescript
class Rectangle {
    protected width: number;
    protected height: number;

    setWidth(width: number) { this.width = width; }
    setHeight(height: number) { this.height = height; }
    getArea(): number { return this.width * this.height; }
}

class Square extends Rectangle {
    // Violates LSP: Changing behavior unexpectedly
    setWidth(width: number) {
        this.width = width;
        this.height = width; // Side effect!
    }

    setHeight(height: number) {
        this.width = height; // Side effect!
        this.height = height;
    }
}

// This breaks when using Square
function resizeRectangle(rect: Rectangle) {
    rect.setWidth(5);
    rect.setHeight(4);
    console.assert(rect.getArea() === 20); // Fails for Square!
}
```

**Good Example:**
```typescript
interface Shape {
    getArea(): number;
}

class Rectangle implements Shape {
    constructor(private width: number, private height: number) {}

    setWidth(width: number) { this.width = width; }
    setHeight(height: number) { this.height = height; }
    getArea(): number { return this.width * this.height; }
}

class Square implements Shape {
    constructor(private size: number) {}

    setSize(size: number) { this.size = size; }
    getArea(): number { return this.size * this.size; }
}
```

### I - Interface Segregation Principle (ISP)

**Definition:** Clients should not be forced to depend on interfaces they don't use. Many specific interfaces are better than one general-purpose interface.

**Why It Matters:**
- Reduces unnecessary dependencies
- Makes code more modular
- Easier to implement and test

**Bad Example:**
```java
interface Worker {
    void work();
    void eat();
    void sleep();
}

class HumanWorker implements Worker {
    public void work() { /* ... */ }
    public void eat() { /* ... */ }
    public void sleep() { /* ... */ }
}

class RobotWorker implements Worker {
    public void work() { /* ... */ }
    public void eat() { /* Robot doesn't eat! */ }
    public void sleep() { /* Robot doesn't sleep! */ }
}
```

**Good Example:**
```java
interface Workable {
    void work();
}

interface Eatable {
    void eat();
}

interface Sleepable {
    void sleep();
}

class HumanWorker implements Workable, Eatable, Sleepable {
    public void work() { /* ... */ }
    public void eat() { /* ... */ }
    public void sleep() { /* ... */ }
}

class RobotWorker implements Workable {
    public void work() { /* ... */ }
}
```

### D - Dependency Inversion Principle (DIP)

**Definition:** High-level modules should not depend on low-level modules. Both should depend on abstractions.

**Why It Matters:**
- Reduces coupling
- Makes code more testable
- Enables easier changes to implementations

**Bad Example:**
```python
class MySQLDatabase:
    def save(self, data):
        # MySQL-specific logic
        pass

class UserService:
    def __init__(self):
        self.database = MySQLDatabase()  # Tight coupling!

    def save_user(self, user):
        self.database.save(user)
```

**Good Example:**
```python
from abc import ABC, abstractmethod

class Database(ABC):
    @abstractmethod
    def save(self, data):
        pass

class MySQLDatabase(Database):
    def save(self, data):
        # MySQL-specific logic
        pass

class PostgreSQLDatabase(Database):
    def save(self, data):
        # PostgreSQL-specific logic
        pass

class UserService:
    def __init__(self, database: Database):
        self.database = database  # Depends on abstraction

    def save_user(self, user):
        self.database.save(user)

# Usage
service = UserService(MySQLDatabase())  # Easy to swap implementations
```

## DRY Principle

**Definition:** Don't Repeat Yourself - Every piece of knowledge should have a single, unambiguous representation in the system.

**Why It Matters:**
- Reduces maintenance burden
- Ensures consistency
- Makes changes easier and safer

**Bad Example:**
```javascript
function calculateOrderTotalForRegularCustomer(items) {
    let total = 0;
    for (let item of items) {
        total += item.price * item.quantity;
    }
    return total * 0.9; // 10% discount
}

function calculateOrderTotalForPremiumCustomer(items) {
    let total = 0;
    for (let item of items) {
        total += item.price * item.quantity;
    }
    return total * 0.8; // 20% discount
}
```

**Good Example:**
```javascript
function calculateSubtotal(items) {
    return items.reduce((total, item) => total + item.price * item.quantity, 0);
}

function applyDiscount(amount, discountRate) {
    return amount * (1 - discountRate);
}

function calculateOrderTotal(items, discountRate) {
    const subtotal = calculateSubtotal(items);
    return applyDiscount(subtotal, discountRate);
}

// Usage
const regularTotal = calculateOrderTotal(items, 0.1);
const premiumTotal = calculateOrderTotal(items, 0.2);
```

## KISS Principle

**Definition:** Keep It Simple, Stupid - Systems work best if they are kept simple rather than made complicated.

**Why It Matters:**
- Easier to understand and maintain
- Fewer bugs
- Faster development
- Better performance

**Bad Example:**
```python
def is_even(number):
    return True if number % 2 == 0 and number != 0 or number == 0 else False if number % 2 != 0 else True
```

**Good Example:**
```python
def is_even(number):
    return number % 2 == 0
```

**Another Example:**

**Bad (Over-engineered):**
```java
// Unnecessary abstraction for simple task
interface AnimalFactory {
    Animal createAnimal();
}

class DogFactory implements AnimalFactory {
    public Animal createAnimal() {
        return new Dog();
    }
}

class CatFactory implements AnimalFactory {
    public Animal createAnimal() {
        return new Cat();
    }
}

// Complex factory system for simple object creation
```

**Good (Simple and direct):**
```java
// Direct object creation when that's all you need
Dog dog = new Dog();
Cat cat = new Cat();
```

## YAGNI Principle

**Definition:** You Aren't Gonna Need It - Don't implement something until it is necessary.

**Why It Matters:**
- Reduces code bloat
- Saves development time
- Avoids maintenance of unused code
- Keeps focus on actual requirements

**Bad Example:**
```typescript
class User {
    private id: number;
    private name: string;
    private email: string;

    // Features that might be needed someday (but aren't needed now)
    private secondaryEmail?: string;
    private phoneNumbers?: string[];
    private socialMediaProfiles?: Map<string, string>;
    private preferences?: UserPreferences;
    private metadata?: Map<string, any>;

    // Methods for features not yet required
    public addSecondaryEmail(email: string) { /* ... */ }
    public removeSecondaryEmail() { /* ... */ }
    public addPhoneNumber(phone: string) { /* ... */ }
    // ... many more unused methods
}
```

**Good Example:**
```typescript
class User {
    private id: number;
    private name: string;
    private email: string;

    // Only what's needed now
    // Add features when actually required
}
```

## Additional Clean Code Principles

### Meaningful Names

**Use Intention-Revealing Names:**
```javascript
// Bad
const d = 86400; // elapsed time in seconds

// Good
const SECONDS_PER_DAY = 86400;
const elapsedTimeInDays = elapsedTimeInSeconds / SECONDS_PER_DAY;
```

**Avoid Disinformation:**
```python
# Bad
accountList = {}  # It's a dict, not a list!

# Good
accounts = {}
accountMap = {}
```

**Make Meaningful Distinctions:**
```java
// Bad
public void copyChars(char a1[], char a2[]) { /* ... */ }

// Good
public void copyChars(char source[], char destination[]) { /* ... */ }
```

**Use Pronounceable Names:**
```javascript
// Bad
const yyyymmdstr = new Date().toISOString().slice(0, 10);

// Good
const currentDate = new Date().toISOString().slice(0, 10);
```

**Use Searchable Names:**
```python
# Bad
if user.status == 4:
    # ...

# Good
USER_STATUS_ACTIVE = 4
if user.status == USER_STATUS_ACTIVE:
    # ...
```

### Functions

**Small Functions:**
- Functions should be small (ideally <20 lines)
- Do one thing and do it well
- One level of abstraction per function

**Descriptive Names:**
```javascript
// Bad
function process(data) { /* ... */ }

// Good
function validateAndSaveUserData(userData) { /* ... */ }
```

**Few Arguments:**
- Ideal: 0 arguments (niladic)
- Good: 1 argument (monadic)
- Acceptable: 2 arguments (dyadic)
- Avoid: 3+ arguments (triadic or polyadic)

**No Side Effects:**
```python
# Bad: Unexpected side effect
def check_password(username, password):
    user = DB.find_user(username)
    if user.password == password:
        Session.initialize()  # Side effect!
        return True
    return False

# Good: Pure function
def is_password_valid(username, password):
    user = DB.find_user(username)
    return user.password == password

def login(username, password):
    if is_password_valid(username, password):
        Session.initialize()
        return True
    return False
```

### Comments

**Good Comments:**
- Legal comments (copyright, license)
- Informative comments (regex explanation)
- Warning of consequences
- TODO comments (with name and date)
- Documentation comments for public APIs

**Bad Comments:**
- Redundant comments
- Misleading comments
- Commented-out code
- Noise comments
- Position markers

```java
// Bad: Redundant comment
// Get the user name
String userName = user.getName();

// Good: Self-documenting code
String userName = user.getName();

// Good: Useful comment explaining why
// Timeout must be longer than the maximum DB replication lag (5 seconds)
final int TIMEOUT_MS = 10000;
```

### Error Handling

**Use Exceptions Rather Than Return Codes:**
```python
# Bad
def save_user(user):
    if not user.is_valid():
        return -1
    if not db.is_connected():
        return -2
    # ...
    return 0

# Good
def save_user(user):
    if not user.is_valid():
        raise ValidationError("Invalid user data")
    if not db.is_connected():
        raise DatabaseError("Database not connected")
    # ...
```

**Don't Return Null:**
```java
// Bad
public List<User> getUsers() {
    if (users.isEmpty()) {
        return null;  // Caller must check for null
    }
    return users;
}

// Good
public List<User> getUsers() {
    return users;  // Return empty list, never null
}
```

**Don't Pass Null:**
```typescript
// Bad
function calculateArea(width: number | null, height: number | null): number {
    if (width === null || height === null) {
        return 0;
    }
    return width * height;
}

// Good
function calculateArea(width: number, height: number): number {
    return width * height;
}
```

### Code Organization

**Vertical Formatting:**
- Related code should be close together
- Variable declarations close to usage
- Dependent functions close to each other
- Similar functions grouped together

**Horizontal Formatting:**
- Lines should be short (<120 characters)
- Use whitespace to associate related things
- Don't use horizontal alignment

**The Newspaper Metaphor:**
- High-level concepts at top
- Details increase as you read down
- Most important information first

### Classes

**Class Organization:**
1. Public static constants
2. Private static variables
3. Private instance variables
4. Public functions
5. Private utilities (called by public functions)

**Small Classes:**
- Classes should be small (measured by responsibilities)
- Single Responsibility Principle
- High cohesion - elements should be related

**Encapsulation:**
```java
// Bad: Exposed internals
public class User {
    public String name;
    public List<String> roles;
}

// Good: Encapsulation
public class User {
    private String name;
    private List<String> roles;

    public String getName() { return name; }
    public void addRole(String role) { roles.add(role); }
    public boolean hasRole(String role) { return roles.contains(role); }
}
```

## Clean Code Checklist

### Naming
- [ ] Names are meaningful and intention-revealing
- [ ] Names are pronounceable and searchable
- [ ] No mental mapping required
- [ ] Classes/objects use nouns
- [ ] Methods use verbs
- [ ] Consistent naming conventions

### Functions
- [ ] Functions are small (<20 lines)
- [ ] Functions do one thing
- [ ] One level of abstraction
- [ ] Descriptive names
- [ ] Few arguments (<3)
- [ ] No side effects

### Comments
- [ ] Code is self-documenting
- [ ] Comments explain "why", not "what"
- [ ] No commented-out code
- [ ] No redundant comments
- [ ] TODO comments have owner

### Structure
- [ ] Proper separation of concerns
- [ ] Appropriate abstraction levels
- [ ] Dependencies point inward
- [ ] Related code is together
- [ ] Consistent formatting

### Error Handling
- [ ] Exceptions over error codes
- [ ] Never return null
- [ ] Never pass null
- [ ] Proper exception hierarchy
- [ ] Meaningful error messages

### Tests
- [ ] Fast, independent, repeatable
- [ ] Self-validating
- [ ] Timely (written with code)
- [ ] One concept per test
- [ ] Readable test names

## Summary

Clean code principles are not rules but guidelines that help write better, more maintainable code. The key is to:

1. **Write for humans first**: Code is read more than written
2. **Keep it simple**: Simple solutions are usually better
3. **Be consistent**: Follow established patterns
4. **Refactor continuously**: Improve code constantly
5. **Test thoroughly**: Tests enable confident refactoring
6. **Review regularly**: Fresh eyes catch issues
7. **Learn continuously**: Principles evolve with experience

Remember: Clean code is not about following rules dogmatically, but about writing code that is easy to understand, maintain, and extend.
