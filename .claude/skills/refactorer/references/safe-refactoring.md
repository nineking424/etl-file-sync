# Safe Refactoring Procedures

A comprehensive guide to performing refactorings safely with minimal risk of introducing defects.

## Table of Contents

1. [Fundamental Safety Principles](#fundamental-safety-principles)
2. [Pre-Refactoring Checklist](#pre-refactoring-checklist)
3. [Test-Driven Refactoring](#test-driven-refactoring)
4. [Risk Assessment Framework](#risk-assessment-framework)
5. [Step-by-Step Safe Procedures](#step-by-step-safe-procedures)
6. [Rollback Procedures](#rollback-procedures)
7. [Version Control Integration](#version-control-integration)
8. [Team Coordination](#team-coordination)

---

## Fundamental Safety Principles

### The Golden Rule of Refactoring

**Never change behavior while refactoring. Never refactor while changing behavior.**

Separate these activities:
- **Refactoring**: Changing structure without changing behavior
- **Feature Addition**: Adding new functionality
- **Bug Fixing**: Correcting incorrect behavior

### The Safety Net: Automated Tests

Tests are your safety harness for refactoring. Without comprehensive tests:
- Do NOT refactor
- First, add tests
- Then refactor

### The Incremental Principle

Make the smallest possible changes that:
- Can be independently verified
- Can be easily understood
- Can be quickly reversed if needed

### The Continuous Verification Principle

After EVERY change:
1. Run relevant tests
2. Verify compilation/syntax
3. Check for unexpected side effects

---

## Pre-Refactoring Checklist

Before starting any refactoring, verify:

### 1. Test Coverage Assessment

- [ ] Existing tests cover the code to be refactored
- [ ] All tests currently pass
- [ ] Tests are fast enough to run frequently
- [ ] Tests are reliable (no flaky tests)

**If tests are insufficient:**
- Add characterization tests
- Document current behavior
- Create regression tests

### 2. Version Control Status

- [ ] Working directory is clean (no uncommitted changes)
- [ ] Current branch is up-to-date with main/master
- [ ] Create a dedicated refactoring branch
- [ ] Branch name clearly indicates refactoring purpose

```bash
# Example branch naming
git checkout -b refactor/extract-user-validation
git checkout -b refactor/simplify-payment-logic
git checkout -b refactor/remove-duplicate-queries
```

### 3. Code Understanding

- [ ] Understand what the code currently does
- [ ] Identify all call sites
- [ ] Map dependencies and dependents
- [ ] Review related documentation

### 4. Environment Preparation

- [ ] Development environment is stable
- [ ] All dependencies are installed and working
- [ ] Build/compile process succeeds
- [ ] Can run test suite successfully

### 5. Time and Resource Planning

- [ ] Have allocated sufficient time
- [ ] Won't be interrupted mid-refactoring
- [ ] Team is aware of refactoring plans
- [ ] Have rollback plan if issues arise

---

## Test-Driven Refactoring

### The Red-Green-Refactor Cycle

```
RED -> GREEN -> REFACTOR -> RED -> GREEN -> REFACTOR ...
```

1. **RED**: Write a failing test
2. **GREEN**: Make it pass with minimal code
3. **REFACTOR**: Improve the code structure

### Applying TDD to Refactoring

#### 1. Add Characterization Tests

If tests don't exist, create tests that document current behavior:

```python
# Characterization test for legacy code
def test_calculate_discount_current_behavior():
    """Documents existing behavior before refactoring."""
    # Test current behavior, even if odd
    result = calculate_discount(100, 'GOLD', 5)
    assert result == 85.5  # Whatever it currently returns
```

#### 2. Verify Tests Pass

```bash
# Run full test suite
pytest

# Or with coverage
pytest --cov=src --cov-report=term-missing
```

#### 3. Refactor in Small Steps

Each step:
- Change one thing
- Run tests
- Commit if green

#### 4. Add Tests for New Structure

After refactoring:
- Add tests that express intent more clearly
- Remove obsolete characterization tests
- Improve test organization

---

## Risk Assessment Framework

### Risk Levels

Categorize each refactoring by risk level:

#### Low Risk (Safe to proceed immediately)

- Rename variable/method/class (with IDE support)
- Extract constant
- Extract variable
- Inline temporary variable
- Add/remove comments
- Format code

**Procedure**:
1. Make change
2. Run tests
3. Commit

#### Medium Risk (Requires careful testing)

- Extract method
- Inline method
- Move method
- Extract class
- Introduce parameter object
- Replace conditional with polymorphism

**Procedure**:
1. Review all call sites
2. Plan incremental steps
3. Make smallest change
4. Run full test suite
5. Commit
6. Repeat

#### High Risk (Requires extensive planning)

- Change bidirectional association to unidirectional
- Replace inheritance with delegation
- Tease apart inheritance
- Extract interface across multiple clients
- Large-scale architectural changes

**Procedure**:
1. Create detailed refactoring plan
2. Communicate with team
3. Consider feature flags
4. Plan rollback strategy
5. Execute in multiple commits
6. Review at each checkpoint
7. Consider pair programming

### Risk Factors

Factors that increase risk:

- **No Test Coverage**: Critical factor - HIGH RISK
- **Public API**: Changes affect external clients - Medium to High
- **Performance Critical**: Could impact benchmarks - Medium
- **Complex Logic**: Hard to verify correctness - Medium
- **Multiple Call Sites**: Many places affected - Medium
- **Cross-Module Changes**: Affects many files - Medium to High
- **Production Incidents History**: Area has had bugs - Medium to High

---

## Step-by-Step Safe Procedures

### Procedure 1: Extract Method (Medium Risk)

**Goal**: Extract code fragment into a new method

**Steps**:

1. **Identify Code to Extract**
   - Select self-contained code fragment
   - Verify it's a logical unit
   - Check for dependencies

2. **Analyze Variables**
   - List all local variables used
   - Identify variables modified
   - Determine parameters and return values

3. **Create New Method**
   ```python
   # Step 1: Create method signature
   def new_method_name():
       pass
   ```

4. **Copy Code**
   ```python
   # Step 2: Copy code to new method
   def new_method_name():
       # ... copied code ...
       pass
   ```

5. **Add Parameters**
   ```python
   # Step 3: Add necessary parameters
   def new_method_name(param1, param2):
       # ... code using param1, param2 ...
       pass
   ```

6. **Add Return Statement**
   ```python
   # Step 4: Return computed values
   def new_method_name(param1, param2):
       result = # ... computation ...
       return result
   ```

7. **Replace Original Code**
   ```python
   # Step 5: Call new method
   result = new_method_name(arg1, arg2)
   ```

8. **Test**
   ```bash
   # Run tests
   pytest test_module.py -v
   ```

9. **Commit**
   ```bash
   git add .
   git commit -m "Extract method: new_method_name from original_method"
   ```

### Procedure 2: Rename Method (Low Risk with IDE)

**Goal**: Change method name to better express intent

**Steps**:

1. **Choose Better Name**
   - Verb + noun form (e.g., `getUserById`, `calculateTotal`)
   - Expresses what method does
   - Follows naming conventions

2. **Use IDE Rename Refactoring**
   - Right-click method name
   - Select "Rename" or "Refactor > Rename"
   - Enter new name
   - Preview changes
   - Apply

3. **Manual Alternative (if no IDE support)**

   a. **Find All Usages**
   ```bash
   # Use grep/ripgrep to find all calls
   rg "old_method_name" --type python
   ```

   b. **Update Each Call Site**
   - Start with tests
   - Then production code
   - Update one file at a time
   - Run tests after each file

   c. **Update Method Definition**
   - Change method signature
   - Update docstrings

   d. **Verify No References Remain**
   ```bash
   rg "old_method_name" --type python
   ```

4. **Test**
   ```bash
   pytest
   ```

5. **Commit**
   ```bash
   git add .
   git commit -m "Rename: old_method_name -> new_method_name"
   ```

### Procedure 3: Extract Class (High Risk)

**Goal**: Split a large class into two classes

**Steps**:

1. **Identify Responsibilities**
   - List all instance variables
   - List all methods
   - Group by cohesion
   - Identify natural boundaries

2. **Plan the Split**
   - Primary class keeps main responsibility
   - New class gets secondary responsibility
   - Define interface between classes

3. **Create New Class**
   ```python
   # Step 1: Create empty class
   class NewClass:
       def __init__(self):
           pass
   ```

4. **Move Fields (One at a Time)**

   a. **Move First Field**
   ```python
   class NewClass:
       def __init__(self):
           self.field1 = None
   ```

   b. **Update Original Class**
   ```python
   class OriginalClass:
       def __init__(self):
           self.new_class = NewClass()
           # self.field1 = None  # Removed
   ```

   c. **Update References**
   - Change `self.field1` to `self.new_class.field1`
   - Update one method at a time

   d. **Test**
   ```bash
   pytest
   ```

   e. **Commit**
   ```bash
   git commit -m "Move field1 to NewClass"
   ```

5. **Move Methods (One at a Time)**

   a. **Move First Method**
   ```python
   class NewClass:
       def method1(self):
           # ... implementation ...
   ```

   b. **Update Original Class to Delegate**
   ```python
   class OriginalClass:
       def method1(self):
           return self.new_class.method1()
   ```

   c. **Test**

   d. **Commit**

6. **Remove Delegation (If Appropriate)**
   - Update callers to use new class directly
   - Remove delegating methods

7. **Final Testing**
   ```bash
   pytest --cov=src
   ```

---

## Rollback Procedures

### When to Rollback

Rollback immediately if:
- Tests fail and can't quickly fix
- Unexpected behavior appears
- Performance degrades significantly
- Running out of time
- Complexity is increasing, not decreasing

### Git Rollback Strategies

#### 1. Uncommitted Changes

```bash
# Discard all uncommitted changes
git reset --hard HEAD

# Discard specific file
git checkout HEAD -- path/to/file.py
```

#### 2. Last Commit Only

```bash
# Undo last commit, keep changes
git reset --soft HEAD~1

# Undo last commit, discard changes
git reset --hard HEAD~1
```

#### 3. Multiple Commits

```bash
# View commit history
git log --oneline

# Reset to specific commit
git reset --hard <commit-hash>
```

#### 4. Already Pushed to Remote

```bash
# Create revert commit
git revert <commit-hash>

# For multiple commits
git revert <oldest-commit>..<newest-commit>
```

### Stash for Later

If refactoring is valuable but not working now:

```bash
# Save current changes
git stash save "WIP: extract user validation"

# List stashes
git stash list

# Restore later
git stash pop
```

---

## Version Control Integration

### Commit Strategy

#### Atomic Commits

Each commit should:
- Complete one refactoring step
- Leave code in working state
- Pass all tests
- Have clear commit message

#### Commit Message Format

```
Refactor: <brief description>

<detailed explanation if needed>

- What was changed
- Why it was changed
- Any trade-offs or decisions made
```

**Examples**:

```
Refactor: Extract validation logic into UserValidator class

Moved all user validation methods from User model into dedicated
UserValidator class to improve separation of concerns.

- Created UserValidator class
- Moved validate_email, validate_age methods
- Updated User model to use UserValidator
- All tests passing
```

```
Refactor: Rename getUserData to fetchUserById

Renamed for clarity - method fetches user from database by ID,
not just retrieving data.
```

### Branch Strategy

#### Option 1: Single Refactoring Branch

```bash
git checkout -b refactor/user-validation
# ... make all refactoring changes ...
git push origin refactor/user-validation
# Create pull request
```

**Pros**: Simple, clear scope
**Cons**: Large PR if many changes

#### Option 2: Multiple Small Branches

```bash
# First step
git checkout -b refactor/extract-user-validator
# ... make changes ...
git push origin refactor/extract-user-validator
# Create PR, merge

# Second step
git checkout main
git pull
git checkout -b refactor/move-validation-methods
# ... continue ...
```

**Pros**: Small PRs, easier review
**Cons**: More overhead, coordination needed

#### Option 3: Feature Flags

For high-risk refactorings:

```python
if feature_flags.new_validation_enabled():
    validator = UserValidator()
    result = validator.validate(user)
else:
    # Old implementation
    result = user.validate()
```

**Pros**: Can deploy incrementally, easy rollback
**Cons**: Temporary code complexity

---

## Team Coordination

### Communication

#### Before Starting

Communicate to team:
- What you're refactoring
- Why it's needed
- Estimated time
- Potential impact on others

**Example Slack/Email**:
```
Hey team, I'm planning to refactor the user validation logic today.

What: Extract validation into UserValidator class
Why: Current User model is too large (500+ lines), hard to test
Impact: User.validate() will still work the same
Timeline: 2-3 hours
Branch: refactor/user-validation

Let me know if this affects anyone's work.
```

#### During Refactoring

- Update team if scope changes
- Alert if taking longer than expected
- Ask for help if stuck

#### After Completion

- Share summary of changes
- Update documentation
- Highlight any new patterns or improvements

### Code Review

#### Refactoring Pull Requests

Make reviewer's job easier:

1. **Clear Description**
   ```markdown
   ## Purpose
   Extract user validation logic into dedicated class

   ## Changes
   - Created UserValidator class
   - Moved 5 validation methods from User model
   - Updated 12 call sites
   - Added tests for UserValidator

   ## Testing
   - All existing tests pass
   - Added new unit tests for UserValidator
   - Manual testing of user registration flow

   ## Before/After
   [Code snippets showing improvement]
   ```

2. **Commits Tell Story**
   - Each commit is a logical step
   - Reviewer can see progression
   - Easy to identify where issue occurred

3. **Automated Checks Pass**
   - Tests green
   - Linter passing
   - Code coverage maintained or improved

### Pair Programming for High-Risk Refactorings

Benefits:
- Real-time review
- Catch mistakes immediately
- Share knowledge
- Higher confidence

Approach:
- Driver: Makes changes
- Navigator: Reviews, suggests, watches for issues
- Switch roles regularly

---

## Monitoring and Metrics

### Pre/Post Refactoring Metrics

Track improvements:

#### Code Complexity
```bash
# Python: radon
radon cc src/ -a

# JavaScript: complexity-report
cr src/
```

#### Code Duplication
```bash
# jscpd (works for many languages)
jscpd src/
```

#### Test Coverage
```bash
# Python
pytest --cov=src --cov-report=term

# JavaScript
npm test -- --coverage
```

#### Lines of Code
```bash
cloc src/
```

### Continuous Monitoring

After deploying refactored code:

- **Error Rates**: Watch for increase in errors
- **Performance**: Monitor response times
- **User Reports**: Track support tickets
- **System Metrics**: CPU, memory usage

### Success Criteria

Refactoring is successful if:

- [ ] All tests pass
- [ ] Code is more understandable
- [ ] Complexity metrics improved
- [ ] No new bugs introduced
- [ ] Performance maintained or improved
- [ ] Team can work faster in refactored area

---

## Emergency Procedures

### If Tests Break During Refactoring

1. **Don't Panic**: Tests breaking is expected, shows they work
2. **Diagnose**:
   - Read test failure message carefully
   - Identify what changed
   - Determine if test or code is wrong
3. **Fix**:
   - If code wrong: fix the code
   - If test wrong: update the test
4. **Verify**: Run tests again

### If Production Bug Found After Refactoring

1. **Assess Severity**
   - Critical: Immediate rollback
   - High: Quick fix or rollback
   - Medium/Low: Fix in next deploy

2. **Immediate Actions**:
   ```bash
   # Rollback option
   git revert <refactoring-commit>
   git push

   # Or quick fix
   # Make minimal fix
   git commit -m "Hotfix: <issue>"
   git push
   ```

3. **Root Cause Analysis**:
   - What was missed in testing?
   - How did it get through review?
   - What can prevent this in future?

4. **Improve Process**:
   - Add missing test cases
   - Update refactoring procedures
   - Share learnings with team

---

## Conclusion

Safe refactoring is about:
- **Discipline**: Follow procedures even when tedious
- **Testing**: Comprehensive automated tests
- **Incrementalism**: Small, verifiable steps
- **Reversibility**: Always able to rollback
- **Communication**: Keep team informed

**Remember**: It's better to not refactor than to refactor unsafely.

When in doubt:
1. Add more tests first
2. Make smaller changes
3. Ask for pair programming
4. Get early review

*Safe refactoring is a skill that improves with practice and discipline.*
