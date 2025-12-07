# Code Smells Catalog

A comprehensive guide to identifying and addressing common code smells. Code smells are indicators of deeper problems in the code that may require refactoring.

## 1. Bloaters

Code, methods, and classes that have increased to such proportions that they are hard to work with.

### Long Method
**Symptoms:**
- Methods with more than 50 lines of code
- Methods that do multiple things
- Methods that are hard to name concisely

**Detection:**
- Count lines of code in method
- Check cyclomatic complexity
- Assess number of responsibilities

**Refactoring:**
- Extract Method: Break into smaller methods
- Replace Temp with Query: Remove temporary variables
- Introduce Parameter Object: Group parameters
- Preserve Whole Object: Pass object instead of fields

**Example:**
```javascript
// Bad: Long method doing too much
function processOrder(orderId, userId, items, discount, shipping) {
  // 100+ lines of validation, calculation, database operations
}

// Good: Broken into focused methods
function processOrder(orderId, userId, orderData) {
  validateOrder(orderData);
  const total = calculateTotal(orderData);
  const finalPrice = applyDiscounts(total, orderData.discount);
  saveOrder(orderId, userId, finalPrice);
  notifyCustomer(userId, orderId);
}
```

### Large Class
**Symptoms:**
- Classes with too many instance variables
- Classes with too many methods
- Classes doing too much (God Object)

**Detection:**
- Count instance variables (>10 is suspicious)
- Count methods (>20 is suspicious)
- Check if class name is vague (e.g., "Manager", "Helper")

**Refactoring:**
- Extract Class: Split into multiple classes
- Extract Subclass: Move behavior to subclass
- Extract Interface: Define cleaner contract
- Replace Data Value with Object: Encapsulate data

### Primitive Obsession
**Symptoms:**
- Using primitives instead of small objects
- Using constants for coded information
- Using string constants as field names

**Detection:**
- Type codes or magic numbers
- String parsing for structured data
- Field names as strings

**Refactoring:**
- Replace Type Code with Class
- Replace Type Code with Subclasses
- Introduce Parameter Object
- Replace Array with Object

**Example:**
```typescript
// Bad: Primitive obsession
function createUser(name: string, email: string, role: number) {
  // role: 0 = admin, 1 = user, 2 = guest
}

// Good: Use value objects
enum UserRole { Admin, User, Guest }
class Email {
  constructor(private value: string) {
    if (!this.isValid(value)) throw new Error('Invalid email');
  }
  isValid(email: string): boolean { /* validation */ }
}
function createUser(name: string, email: Email, role: UserRole) {
  // ...
}
```

### Long Parameter List
**Symptoms:**
- Methods with more than 3-4 parameters
- Parameters that are related to each other
- Parameters that are always passed together

**Detection:**
- Count method parameters
- Look for groups of related parameters
- Check if parameters are from same object

**Refactoring:**
- Replace Parameter with Method Call
- Preserve Whole Object
- Introduce Parameter Object

### Data Clumps
**Symptoms:**
- Same group of data items together in multiple places
- Parameters that appear together repeatedly
- Fields that are deleted together

**Detection:**
- Look for repeated parameter groups
- Check for similar field groups in multiple classes
- Identify data that moves together

**Refactoring:**
- Extract Class
- Introduce Parameter Object
- Preserve Whole Object

## 2. Object-Orientation Abusers

Incomplete or incorrect application of object-oriented programming principles.

### Switch Statements
**Symptoms:**
- Large switch/case statements
- Type checking followed by different behavior
- Same switch logic repeated in multiple places

**Detection:**
- Count switch/case statements
- Look for type checking patterns
- Find duplicate switch logic

**Refactoring:**
- Replace Conditional with Polymorphism
- Replace Type Code with State/Strategy
- Replace Parameter with Explicit Methods
- Introduce Null Object

**Example:**
```python
# Bad: Switch statement
def get_area(shape):
    if shape.type == 'circle':
        return 3.14 * shape.radius ** 2
    elif shape.type == 'rectangle':
        return shape.width * shape.height
    elif shape.type == 'triangle':
        return 0.5 * shape.base * shape.height

# Good: Polymorphism
class Shape:
    def get_area(self):
        raise NotImplementedError

class Circle(Shape):
    def get_area(self):
        return 3.14 * self.radius ** 2

class Rectangle(Shape):
    def get_area(self):
        return self.width * self.height
```

### Temporary Field
**Symptoms:**
- Fields that are only set in certain circumstances
- Fields that are empty most of the time
- Complex algorithms using instance variables for intermediate values

**Detection:**
- Look for optional fields
- Check for fields only used in few methods
- Find fields set in one method, used in another

**Refactoring:**
- Extract Class
- Introduce Null Object
- Replace Method with Method Object

### Refused Bequest
**Symptoms:**
- Subclass uses only some methods/properties of parent
- Subclass overrides most parent methods
- Inheritance hierarchy doesn't make logical sense

**Detection:**
- Count overridden methods that throw exceptions
- Check if subclass uses parent's interface
- Verify is-a relationship

**Refactoring:**
- Replace Inheritance with Delegation
- Extract Subclass
- Push Down Method/Field

### Alternative Classes with Different Interfaces
**Symptoms:**
- Two classes that do similar things but have different method names
- Duplicate functionality in different classes
- Similar logic with different interfaces

**Detection:**
- Compare method signatures across classes
- Look for similar algorithms
- Find duplicate functionality

**Refactoring:**
- Rename Method
- Move Method
- Extract Superclass

## 3. Change Preventers

These smells mean that if you need to change something in one place, you have to make many changes in other places.

### Divergent Change
**Symptoms:**
- One class changed for many different reasons
- Each type of change affects different methods
- Multiple teams modify the same class

**Detection:**
- Track why class changes
- Count different types of modifications
- Check change frequency

**Refactoring:**
- Extract Class
- Split Phase
- Move Function

**Example:**
```java
// Bad: Class changes for multiple reasons
class User {
    void save() { /* database logic */ }
    void validate() { /* validation logic */ }
    void sendEmail() { /* email logic */ }
    void formatForDisplay() { /* display logic */ }
}

// Good: Separated concerns
class User {
    private UserValidator validator;
    private UserRepository repository;
    private EmailService emailService;
    private UserFormatter formatter;
}
```

### Shotgun Surgery
**Symptoms:**
- Single change requires modifications in many classes
- Small changes require touching many files
- Changes are scattered across the codebase

**Detection:**
- Track how many files change together
- Look for coupled changes
- Check dependency graphs

**Refactoring:**
- Move Method
- Move Field
- Inline Class
- Pull Up Method

### Parallel Inheritance Hierarchies
**Symptoms:**
- Creating subclass requires creating subclass in another hierarchy
- Two hierarchies with parallel structure
- Changes in one hierarchy require changes in another

**Detection:**
- Map inheritance hierarchies
- Look for matching class names
- Check for synchronized changes

**Refactoring:**
- Move Method
- Move Field
- Collapse Hierarchy

## 4. Dispensables

Something pointless and unneeded whose absence would make the code cleaner.

### Comments
**Symptoms:**
- Comments explaining what code does
- Comments apologizing for bad code
- Outdated comments
- Comments for obvious things

**Detection:**
- Look for comment density
- Check if comments explain "what" instead of "why"
- Find comments that contradict code

**Refactoring:**
- Extract Method
- Rename Method
- Introduce Assertion
- Remove comments and write self-documenting code

**Example:**
```javascript
// Bad: Comment explaining what
// Loop through users array and check if email exists
for (let i = 0; i < users.length; i++) {
    if (users[i].email === email) return true;
}

// Good: Self-documenting code
function hasUserWithEmail(users, email) {
    return users.some(user => user.email === email);
}
```

### Duplicate Code
**Symptoms:**
- Same code structure in multiple places
- Copy-paste programming
- Similar algorithms with slight variations

**Detection:**
- Use code duplication detection tools
- Look for similar method bodies
- Find repeated patterns

**Refactoring:**
- Extract Method
- Pull Up Method
- Form Template Method
- Substitute Algorithm

### Lazy Class
**Symptoms:**
- Class that doesn't do enough to justify existence
- Class with only a few methods
- Class created for future functionality that never came

**Detection:**
- Count methods and fields
- Check lines of code
- Assess if class adds value

**Refactoring:**
- Inline Class
- Collapse Hierarchy
- Remove class entirely

### Dead Code
**Symptoms:**
- Unused variables
- Unreachable code
- Unused parameters
- Unused methods or classes

**Detection:**
- Use static analysis tools
- Check for unreachable branches
- Look for commented code
- Find unused imports

**Refactoring:**
- Delete it
- Remove unused parameters
- Inline if needed

### Speculative Generality
**Symptoms:**
- Code designed for future scenarios that don't exist
- Unused abstract classes or interfaces
- Overly complex design for simple requirements

**Detection:**
- Find unused extension points
- Look for single implementation interfaces
- Check for "just in case" code

**Refactoring:**
- Collapse Hierarchy
- Inline Class
- Remove Parameter
- Rename Method

## 5. Couplers

These smells contribute to excessive coupling between classes.

### Feature Envy
**Symptoms:**
- Method uses more features of another class than its own
- Method seems to belong to another class
- Excessive calls to other objects

**Detection:**
- Count method calls to other classes
- Compare with calls to own class
- Check where data comes from

**Refactoring:**
- Move Method
- Move Field
- Extract Method

**Example:**
```python
# Bad: Feature envy
class Order:
    def get_total_price(self):
        total = 0
        for item in self.items:
            total += item.product.price * item.quantity * \
                    (1 - item.product.discount)
        return total

# Good: Moved to appropriate class
class OrderItem:
    def get_price(self):
        return self.product.price * self.quantity * \
               (1 - self.product.discount)

class Order:
    def get_total_price(self):
        return sum(item.get_price() for item in self.items)
```

### Inappropriate Intimacy
**Symptoms:**
- Classes that know too much about each other's internals
- Classes that access each other's private fields
- Bidirectional dependencies

**Detection:**
- Check field access patterns
- Look for protected field access
- Find circular dependencies

**Refactoring:**
- Move Method
- Move Field
- Change Bidirectional Association to Unidirectional
- Extract Class
- Hide Delegate

### Message Chains
**Symptoms:**
- Long chains of method calls (a.b().c().d())
- Client navigating through object structure
- Law of Demeter violations

**Detection:**
- Count chained calls
- Look for multiple dots
- Check knowledge of object structure

**Refactoring:**
- Hide Delegate
- Extract Method
- Move Method

**Example:**
```java
// Bad: Message chain
customer.getAddress().getCity().getState().getName()

// Good: Hide delegate
customer.getStateName()
```

### Middle Man
**Symptoms:**
- Class that only delegates to another class
- Most methods just call methods on another object
- Excessive use of delegation

**Detection:**
- Count delegating methods
- Check if class adds value
- Look for wrapper classes

**Refactoring:**
- Remove Middle Man
- Inline Method
- Replace Delegation with Inheritance

## 6. Other Common Code Smells

### Magic Numbers
**Symptoms:**
- Hardcoded numeric constants
- Unclear purpose of numbers
- Same number repeated in multiple places

**Detection:**
- Look for literal numbers in code
- Find repeated constants
- Check for unexplained values

**Refactoring:**
- Replace Magic Number with Symbolic Constant
- Use named constants or enums

### Inconsistent Names
**Symptoms:**
- Similar concepts with different names
- Same thing called different names
- Unclear or misleading names

**Detection:**
- Look for naming patterns
- Check for synonyms
- Find unclear abbreviations

**Refactoring:**
- Rename Method/Variable/Class
- Use consistent naming conventions

### Flag Arguments
**Symptoms:**
- Boolean parameters controlling behavior
- Multiple boolean parameters
- If statements based on boolean parameters

**Detection:**
- Count boolean parameters
- Look for behavior switching on booleans
- Find methods with "and" in name

**Refactoring:**
- Split Method
- Replace Parameter with Explicit Methods

**Example:**
```typescript
// Bad: Flag argument
function renderPage(isAdmin: boolean) {
    if (isAdmin) {
        // render admin page
    } else {
        // render user page
    }
}

// Good: Explicit methods
function renderAdminPage() { /* ... */ }
function renderUserPage() { /* ... */ }
```

### Global State
**Symptoms:**
- Global variables
- Singletons used excessively
- Static fields holding state

**Detection:**
- Find global variables
- Count singleton usage
- Look for static mutable state

**Refactoring:**
- Dependency Injection
- Pass parameters explicitly
- Use instance variables

## Detection Strategies

### Automated Tools
- **Linters**: ESLint, Pylint, RuboCop, etc.
- **Static analyzers**: SonarQube, CodeClimate, PMD
- **Complexity tools**: McCabe, radon, lizard
- **Duplication detectors**: CPD, jscpd

### Manual Review Techniques
1. **Code Reading**: Read code like a story
2. **Naming Analysis**: Check if names reveal intent
3. **Dependency Mapping**: Visualize relationships
4. **Change History**: Review why code changed
5. **Test Difficulty**: Hard to test = code smell

### Metrics to Watch
- **Cyclomatic Complexity**: >10 is high
- **Lines of Code**: Methods >50, Classes >500
- **Coupling**: Count dependencies
- **Cohesion**: Check if members relate
- **Test Coverage**: <80% is concerning

## When to Refactor

### Always Refactor
- Before adding new features to smelly code
- When fixing bugs in problematic areas
- During regular maintenance windows

### Sometimes Refactor
- When improving performance
- When code is touched frequently
- When onboarding new team members

### Don't Refactor
- Code that works and never changes
- Third-party libraries
- When under tight deadlines (plan for later)

## Best Practices

1. **Boy Scout Rule**: Leave code better than you found it
2. **Small Steps**: Refactor incrementally
3. **Test First**: Ensure tests pass before and after
4. **One Smell at a Time**: Focus on one issue
5. **Version Control**: Commit frequently
6. **Team Agreement**: Follow team standards
7. **Document Why**: Explain refactoring decisions
8. **Performance Check**: Ensure refactoring doesn't hurt performance

## Summary

Code smells are not bugs—they don't prevent the program from functioning. However, they indicate weaknesses in design that may slow down development or increase the risk of bugs in the future. Regular refactoring to address code smells keeps the codebase healthy and maintainable.
