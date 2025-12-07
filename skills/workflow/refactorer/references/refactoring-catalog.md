# Refactoring Catalog

A comprehensive reference of refactoring patterns based on Martin Fowler's catalog and modern software engineering practices.

## Table of Contents

1. [Method Refactorings](#method-refactorings)
2. [Class Refactorings](#class-refactorings)
3. [Data Refactorings](#data-refactorings)
4. [Conditional Logic Refactorings](#conditional-logic-refactorings)
5. [API Refactorings](#api-refactorings)
6. [Inheritance Refactorings](#inheritance-refactorings)
7. [Code Smell Identification](#code-smell-identification)

---

## Method Refactorings

### Extract Method / Extract Function

**Problem**: Code fragment that can be grouped together

**Solution**: Turn the fragment into a method whose name explains its purpose

**When to Use**:
- Method is too long (> 20-30 lines)
- Code needs a comment to explain what it does
- Code is duplicated in multiple places

**Mechanics**:
1. Create a new method with a name that describes what it does
2. Copy the extracted code to the new method
3. Scan for local variables used in the extracted code
4. Pass local variables as parameters or return values
5. Replace extracted code with call to new method
6. Test

**Example**:

```python
# Before
def print_owing(invoice):
    outstanding = 0

    print("*" * 25)
    print("**** Customer Owes ****")
    print("*" * 25)

    for order in invoice.orders:
        outstanding += order.amount

    print(f"name: {invoice.customer}")
    print(f"amount: {outstanding}")

# After
def print_owing(invoice):
    print_banner()
    outstanding = calculate_outstanding(invoice)
    print_details(invoice, outstanding)

def print_banner():
    print("*" * 25)
    print("**** Customer Owes ****")
    print("*" * 25)

def calculate_outstanding(invoice):
    return sum(order.amount for order in invoice.orders)

def print_details(invoice, outstanding):
    print(f"name: {invoice.customer}")
    print(f"amount: {outstanding}")
```

---

### Inline Method / Inline Function

**Problem**: Method body is as clear as its name

**Solution**: Put the method's body into the body of its callers and remove the method

**When to Use**:
- Method body is obvious and simple
- Method is not overridden
- Indirection is not adding value

**Mechanics**:
1. Check method is not polymorphic
2. Find all calls to the method
3. Replace each call with the method body
4. Test after each replacement
5. Remove the method definition

---

### Extract Variable

**Problem**: Expression is hard to understand

**Solution**: Put the result of the expression into a temporary variable with a descriptive name

**When to Use**:
- Complex boolean condition
- Long calculation expression
- Expression appears multiple times

**Example**:

```javascript
// Before
return (platform.toUpperCase().indexOf("MAC") > -1) &&
       (browser.toUpperCase().indexOf("IE") > -1) &&
       wasInitialized() && resize > 0;

// After
const isMacOS = platform.toUpperCase().indexOf("MAC") > -1;
const isIEBrowser = browser.toUpperCase().indexOf("IE") > -1;
const wasResized = resize > 0;

return isMacOS && isIEBrowser && wasInitialized() && wasResized;
```

---

### Replace Temp with Query

**Problem**: Temporary variable holds the result of an expression

**Solution**: Extract the expression into a method and replace all references to the temp with the expression

**When to Use**:
- Same calculation is needed in multiple places
- Temp obscures logic flow
- Preparing for Extract Method

---

### Split Temporary Variable

**Problem**: Temporary variable assigned to more than once (not a loop variable or collecting variable)

**Solution**: Make a separate temporary variable for each assignment

**When to Use**:
- Variable is used for multiple purposes
- Variable name doesn't match all its uses
- Each assignment represents a different concept

---

## Class Refactorings

### Extract Class

**Problem**: Class doing work that should be done by two classes

**Solution**: Create a new class and move relevant fields and methods to it

**When to Use**:
- Class has too many methods
- Class has too many instance variables
- Subset of data/methods always used together
- Clear separation of responsibilities

**Example**:

```java
// Before
class Person {
    private String name;
    private String officeAreaCode;
    private String officeNumber;

    public String getTelephoneNumber() {
        return "(" + officeAreaCode + ") " + officeNumber;
    }
}

// After
class Person {
    private String name;
    private TelephoneNumber officeTelephone = new TelephoneNumber();

    public String getTelephoneNumber() {
        return officeTelephone.getTelephoneNumber();
    }
}

class TelephoneNumber {
    private String areaCode;
    private String number;

    public String getTelephoneNumber() {
        return "(" + areaCode + ") " + number;
    }
}
```

---

### Inline Class

**Problem**: Class isn't doing very much

**Solution**: Move all its features into another class and delete it

**When to Use**:
- Class has few responsibilities
- After refactoring, class has become too small
- Two classes collaborating too intimately

---

### Hide Delegate

**Problem**: Client is calling a delegate class of an object

**Solution**: Create methods on the server to hide the delegate

**When to Use**:
- Reducing coupling between classes
- Encapsulating navigation paths
- Simplifying client code

---

### Remove Middle Man

**Problem**: Class is doing too much simple delegation

**Solution**: Get the client to call the delegate directly

**When to Use**:
- Too many delegating methods
- Delegation is not adding value
- Inverse of Hide Delegate when taken too far

---

## Data Refactorings

### Encapsulate Field

**Problem**: Public field

**Solution**: Make it private and provide accessors

**When to Use**:
- Always (as a general principle)
- Preparing for data validation
- Preparing for change notification
- Transitioning from public to controlled access

---

### Replace Data Value with Object

**Problem**: Data item needs additional data or behavior

**Solution**: Turn the data item into an object

**When to Use**:
- Simple data item needs validation
- Data has associated behavior
- Data needs formatting logic
- Multiple related data items always travel together

**Example**:

```typescript
// Before
class Order {
    private customer: string;

    constructor(customerName: string) {
        this.customer = customerName;
    }
}

// After
class Order {
    private customer: Customer;

    constructor(customerName: string) {
        this.customer = new Customer(customerName);
    }
}

class Customer {
    private readonly name: string;

    constructor(name: string) {
        this.name = name;
    }

    getName(): string {
        return this.name;
    }
}
```

---

### Change Value to Reference

**Problem**: Many equal instances of a class that need to be one object

**Solution**: Turn the object into a reference object

**When to Use**:
- Need single source of truth
- Objects represent real-world entities
- Need to share updates across instances

---

### Change Reference to Value

**Problem**: Reference object is small, immutable, and awkward to manage

**Solution**: Turn it into a value object

**When to Use**:
- Object is immutable
- No need to share identity
- Simpler code with value semantics

---

## Conditional Logic Refactorings

### Decompose Conditional

**Problem**: Complicated conditional (if-then-else) statement

**Solution**: Extract methods from the condition, then part, and else part

**Example**:

```python
# Before
if date.before(SUMMER_START) or date.after(SUMMER_END):
    charge = quantity * winter_rate + winter_service_charge
else:
    charge = quantity * summer_rate

# After
if is_not_summer(date):
    charge = winter_charge(quantity)
else:
    charge = summer_charge(quantity)

def is_not_summer(date):
    return date.before(SUMMER_START) or date.after(SUMMER_END)

def winter_charge(quantity):
    return quantity * winter_rate + winter_service_charge

def summer_charge(quantity):
    return quantity * summer_rate
```

---

### Consolidate Conditional Expression

**Problem**: Series of conditional tests with the same result

**Solution**: Combine them into a single conditional expression and extract it

**Example**:

```javascript
// Before
if (employee.seniority < 2) return 0;
if (employee.monthsDisabled > 12) return 0;
if (employee.isPartTime) return 0;

// After
if (isNotEligibleForDisability()) return 0;

function isNotEligibleForDisability() {
    return employee.seniority < 2 ||
           employee.monthsDisabled > 12 ||
           employee.isPartTime;
}
```

---

### Replace Nested Conditional with Guard Clauses

**Problem**: Conditional behavior obscures the normal path of execution

**Solution**: Use guard clauses for all special cases

**Example**:

```ruby
# Before
def payment
  if @dead
    dead_amount
  else
    if @separated
      separated_amount
    else
      if @retired
        retired_amount
      else
        normal_amount
      end
    end
  end
end

# After
def payment
  return dead_amount if @dead
  return separated_amount if @separated
  return retired_amount if @retired
  normal_amount
end
```

---

### Replace Conditional with Polymorphism

**Problem**: Conditional chooses different behavior depending on object type

**Solution**: Move each leg of the conditional to an overriding method in a subclass

**When to Use**:
- Type checking code (instanceof, typeof)
- Switch statements based on type
- Different behavior based on object state

---

## API Refactorings

### Rename Method

**Problem**: Method name doesn't reveal its purpose

**Solution**: Change the name of the method

**When to Use**:
- Always (most important refactoring)
- Method name is unclear
- Better name becomes apparent
- Aligning with naming conventions

---

### Add Parameter

**Problem**: Method needs more information from its caller

**Solution**: Add a parameter for the object that can pass on this information

**Caution**: Consider alternatives first (Replace Parameter with Method Call, Preserve Whole Object)

---

### Remove Parameter

**Problem**: Parameter is no longer used by the method body

**Solution**: Remove it

**When to Use**:
- Parameter is truly unused
- After refactoring eliminates need

---

### Separate Query from Modifier

**Problem**: Method returns a value and changes object state

**Solution**: Create two methods, one for the query and one for the modification

**When to Use**:
- Method has side effects and returns a value
- Following Command-Query Separation principle

---

### Parameterize Method

**Problem**: Several methods do similar things with different values

**Solution**: Create one method that uses a parameter for the different values

**Example**:

```java
// Before
public void tenPercentRaise() {
    salary *= 1.1;
}

public void fivePercentRaise() {
    salary *= 1.05;
}

// After
public void raise(double factor) {
    salary *= (1 + factor);
}
```

---

### Replace Parameter with Explicit Methods

**Problem**: Method runs different code depending on parameter values

**Solution**: Create a separate method for each value of the parameter

**When to Use**:
- Parameter values are discrete and known
- Conditional logic based on parameter
- Improved clarity with explicit methods

---

## Inheritance Refactorings

### Pull Up Method

**Problem**: Methods with identical results in subclasses

**Solution**: Move them to the superclass

---

### Pull Up Field

**Problem**: Two subclasses have the same field

**Solution**: Move the field to the superclass

---

### Push Down Method

**Problem**: Behavior on a superclass is relevant only for some subclasses

**Solution**: Move it to those subclasses

---

### Push Down Field

**Problem**: Field is used only by some subclasses

**Solution**: Move the field to those subclasses

---

### Extract Subclass

**Problem**: Class has features used only in some instances

**Solution**: Create a subclass for that subset of features

---

### Extract Superclass

**Problem**: Two classes have similar features

**Solution**: Create a superclass and move common features to it

---

### Replace Inheritance with Delegation

**Problem**: Subclass uses only part of superclass interface or doesn't want to inherit data

**Solution**: Create a field for the superclass, adjust methods to delegate, and remove subclassing

**When to Use**:
- Violating Liskov Substitution Principle
- Inheriting unwanted methods
- Composition is more appropriate

---

### Replace Delegation with Inheritance

**Problem**: Using delegation with many simple delegations

**Solution**: Make the delegating class a subclass of the delegate

**When to Use**:
- Using all methods of delegate
- No need to hide delegate interface
- True "is-a" relationship exists

---

## Code Smell Identification

### Bloaters

**Long Method**: Method contains too many lines of code
- **Threshold**: > 20-30 lines
- **Refactorings**: Extract Method, Replace Temp with Query, Decompose Conditional

**Large Class**: Class contains too many fields/methods/lines
- **Threshold**: > 200-300 lines, > 10 instance variables
- **Refactorings**: Extract Class, Extract Subclass, Extract Interface

**Primitive Obsession**: Using primitives instead of small objects
- **Indicators**: Using strings for common concepts, type codes
- **Refactorings**: Replace Data Value with Object, Introduce Parameter Object

**Long Parameter List**: Method has more than 3-4 parameters
- **Threshold**: > 3-4 parameters
- **Refactorings**: Replace Parameter with Method Call, Introduce Parameter Object, Preserve Whole Object

**Data Clumps**: Groups of data items often appear together
- **Indicators**: Same fields in multiple classes, same parameters in multiple methods
- **Refactorings**: Extract Class, Introduce Parameter Object

---

### Object-Orientation Abusers

**Switch Statements**: Complex switch or if/else chains based on type
- **Refactorings**: Replace Conditional with Polymorphism, Replace Type Code with State/Strategy

**Temporary Field**: Field used only in certain circumstances
- **Refactorings**: Extract Class, Replace Method with Method Object

**Refused Bequest**: Subclass uses only some methods/properties of parent
- **Refactorings**: Replace Inheritance with Delegation, Extract Subclass

**Alternative Classes with Different Interfaces**: Two classes perform identical functions with different signatures
- **Refactorings**: Rename Method, Move Method, Extract Superclass

---

### Change Preventers

**Divergent Change**: One class commonly changed in different ways for different reasons
- **Refactorings**: Extract Class, Split Phase

**Shotgun Surgery**: Making a change requires many small changes to many classes
- **Refactorings**: Move Method, Move Field, Inline Class

**Parallel Inheritance Hierarchies**: Creating subclass requires creating subclass in another hierarchy
- **Refactorings**: Move Method, Move Field

---

### Dispensables

**Comments**: Code needs comments to explain what it does
- **Refactorings**: Extract Method, Rename Method, Introduce Assertion

**Duplicate Code**: Same code structure in multiple places
- **Refactorings**: Extract Method, Pull Up Method, Form Template Method

**Lazy Class**: Class isn't doing enough to justify its existence
- **Refactorings**: Inline Class, Collapse Hierarchy

**Dead Code**: Variable, parameter, field, method, or class no longer used
- **Refactorings**: Delete it

**Speculative Generality**: Element exists "just in case" but isn't used
- **Refactorings**: Collapse Hierarchy, Inline Class, Remove Parameter

---

### Couplers

**Feature Envy**: Method uses data from another object more than its own
- **Refactorings**: Move Method, Extract Method

**Inappropriate Intimacy**: Class uses internal fields/methods of another class
- **Refactorings**: Move Method, Move Field, Change Bidirectional Association to Unidirectional

**Message Chains**: Client navigates through series of objects (a.getB().getC().getD())
- **Refactorings**: Hide Delegate, Extract Method

**Middle Man**: Class delegates most of its work to another class
- **Refactorings**: Remove Middle Man, Inline Method, Replace Delegation with Inheritance

---

## Pattern Application Guidelines

### Selection Process

1. **Identify the Code Smell**: What problem are you solving?
2. **Choose Appropriate Refactoring(s)**: Match smell to catalog entry
3. **Verify Preconditions**: Ensure refactoring is safe to apply
4. **Plan Incremental Steps**: Break down into small changes
5. **Execute with Testing**: Test after each step

### Combination Patterns

Some refactorings work well in sequence:

- **Extract Method + Move Method**: First extract, then move to proper class
- **Extract Variable + Extract Method**: Clarify expression, then extract
- **Rename + Extract**: Rename before extracting for clarity
- **Extract Class + Hide Delegate**: Create class, then hide its details

### When to Stop

- Code is clear and understandable
- Each class has a single responsibility
- Methods are short and focused
- No obvious duplication
- Easy to add new features
- Tests are comprehensive and passing

Avoid over-engineering or premature abstraction.

---

*This catalog is a living document. Patterns should be applied judiciously based on context and actual needs, not dogmatically.*
