# Migration Patterns

Strategies and patterns for modernizing legacy code, migrating to new architectures, and incrementally improving large codebases.

## Table of Contents

1. [Legacy Code Modernization](#legacy-code-modernization)
2. [Incremental Migration Strategies](#incremental-migration-strategies)
3. [Strangler Fig Pattern](#strangler-fig-pattern)
4. [Feature Toggle Approaches](#feature-toggle-approaches)
5. [Branch by Abstraction](#branch-by-abstraction)
6. [Parallel Run Pattern](#parallel-run-pattern)
7. [Database Migration Strategies](#database-migration-strategies)
8. [API Versioning and Migration](#api-versioning-and-migration)

---

## Legacy Code Modernization

### Understanding Legacy Code

**Definition**: Code without adequate tests, hard to understand, difficult to modify

**Characteristics**:
- No or poor test coverage
- Unclear architecture
- Outdated dependencies
- Complex dependencies
- Poor documentation
- Multiple responsibilities per module

### The Modernization Approach

#### Phase 1: Stabilization

**Goal**: Make code safe to modify

1. **Add Characterization Tests**
   ```python
   # Document current behavior, even if wrong
   def test_legacy_calculation_behavior():
       """Documents existing behavior before modernization."""
       result = legacy_calculate(100, 'TYPE_A', True)
       # Assert whatever it currently does
       assert result == 127.50
   ```

2. **Create Test Harness**
   - Extract interfaces where possible
   - Use dependency injection
   - Mock external dependencies
   - Build safety net of tests

3. **Fix Critical Bugs**
   - Address security vulnerabilities
   - Fix data corruption issues
   - Resolve critical performance problems

#### Phase 2: Understanding

1. **Map Architecture**
   - Identify main components
   - Draw dependency diagrams
   - Document data flow
   - Identify seams (places to break dependencies)

2. **Identify Patterns**
   - Look for domain concepts
   - Find duplicated logic
   - Recognize implicit structures
   - Document business rules

3. **Measure Baseline**
   - Code complexity metrics
   - Test coverage
   - Performance benchmarks
   - Error rates

#### Phase 3: Incremental Improvement

1. **Start with Seams**
   - Extract interfaces at boundaries
   - Create abstractions for external dependencies
   - Isolate core business logic

2. **Apply Boy Scout Rule**
   - Leave code better than you found it
   - Small improvements continuously
   - Refactor as you add features

3. **Modernize Layer by Layer**
   - Start with presentation layer (easy, visible)
   - Then business logic
   - Finally, data layer

---

## Incremental Migration Strategies

### The Anti-Pattern: Big Bang Rewrite

**Don't Do This**:
```
1. Stop all feature development
2. Rewrite entire system from scratch
3. Big-bang cutover
4. Hope it works
```

**Why It Fails**:
- Business moves on while you're rewriting
- Original system gets bug fixes, yours gets stale
- Integration is painful
- All-or-nothing deployment
- High risk

### The Better Way: Incremental Migration

**Principles**:
- Maintain working system throughout
- Migrate one feature/module at a time
- Users see incremental improvements
- Can rollback individual changes
- Low risk

**Strategy**:

1. **Identify Migration Units**
   - Self-contained features
   - Bounded contexts
   - Loosely coupled modules

2. **Prioritize**
   - High value, low complexity first
   - Build momentum with early wins
   - Learn and adjust approach

3. **Create Compatibility Layer**
   - Allow old and new to coexist
   - Route traffic selectively
   - Gradually shift load

4. **Migrate Incrementally**
   - One unit at a time
   - Deploy frequently
   - Monitor and validate

5. **Remove Old Code**
   - After new code proven
   - Clean up compatibility layers
   - Complete the migration

---

## Strangler Fig Pattern

Named after the strangler fig tree that grows around another tree and eventually replaces it.

### Concept

Incrementally replace a legacy system by:
1. Intercepting calls to the old system
2. Routing some calls to new implementation
3. Gradually expanding new system
4. Eventually removing old system

### Implementation

#### Step 1: Create Facade/Proxy

```python
# Step 1: Create proxy that routes to old system
class UserServiceProxy:
    def __init__(self):
        self.legacy_service = LegacyUserService()
        self.new_service = None  # Will implement later

    def get_user(self, user_id):
        # Initially, all traffic goes to legacy
        return self.legacy_service.get_user(user_id)

    def create_user(self, user_data):
        return self.legacy_service.create_user(user_data)
```

#### Step 2: Implement New System

```python
# Step 2: Implement new service
class ModernUserService:
    def __init__(self, database):
        self.db = database

    def get_user(self, user_id):
        # Modern implementation
        return self.db.users.find_by_id(user_id)

    def create_user(self, user_data):
        # Modern implementation with validation
        user = User(**user_data)
        user.validate()
        return self.db.users.save(user)
```

#### Step 3: Route Gradually

```python
# Step 3: Route some traffic to new system
class UserServiceProxy:
    def __init__(self):
        self.legacy_service = LegacyUserService()
        self.new_service = ModernUserService(database)
        self.feature_flags = FeatureFlags()

    def get_user(self, user_id):
        if self.feature_flags.is_enabled('new_user_service', user_id):
            return self.new_service.get_user(user_id)
        else:
            return self.legacy_service.get_user(user_id)

    def create_user(self, user_data):
        if self.feature_flags.is_enabled('new_user_service'):
            return self.new_service.create_user(user_data)
        else:
            return self.legacy_service.create_user(user_data)
```

#### Step 4: Expand New System

```python
# Step 4: Gradually increase traffic to new system
# Via feature flags configuration:
# - Start: 1% of users
# - Week 1: 10% of users
# - Week 2: 50% of users
# - Week 3: 100% of users
```

#### Step 5: Remove Legacy

```python
# Step 5: Once new system fully proven, simplify
class UserService:
    """The proxy becomes the actual service."""
    def __init__(self, database):
        self.db = database

    def get_user(self, user_id):
        return self.db.users.find_by_id(user_id)

    def create_user(self, user_data):
        user = User(**user_data)
        user.validate()
        return self.db.users.save(user)

# Delete LegacyUserService
# Delete proxy layer
# Update all references
```

### Benefits

- Incremental, low-risk migration
- Can pause or rollback at any time
- New and old coexist
- Users see gradual improvements
- Learn and adapt as you go

### Challenges

- Maintaining compatibility layer
- Data synchronization between old/new
- Increased complexity during migration
- Monitoring both systems

---

## Feature Toggle Approaches

### What are Feature Toggles?

Runtime switches that enable/disable features without deployment.

### Types of Toggles for Migration

#### 1. Release Toggles

Control deployment of incomplete features:

```javascript
function processPayment(payment) {
    if (featureToggles.isEnabled('new_payment_processor')) {
        return newPaymentProcessor.process(payment);
    } else {
        return legacyPaymentProcessor.process(payment);
    }
}
```

#### 2. Experiment Toggles (A/B Testing)

Compare old vs. new implementation:

```python
def calculate_recommendations(user):
    variant = ab_test.get_variant('recommendation_algorithm', user.id)

    if variant == 'new':
        return new_recommendation_engine.calculate(user)
    else:
        return legacy_recommendation_engine.calculate(user)
```

#### 3. Ops Toggles

Kill switch for problematic features:

```ruby
def generate_report(data)
  if feature_flags.enabled?(:new_report_generator)
    begin
      NewReportGenerator.generate(data)
    rescue => e
      logger.error("New report generator failed: #{e}")
      # Fallback to old
      LegacyReportGenerator.generate(data)
    end
  else
    LegacyReportGenerator.generate(data)
  end
end
```

#### 4. Permission Toggles

Enable for specific users/groups:

```typescript
function useNewEditor(user: User): boolean {
    return featureFlags.isEnabledForUser('new_editor', user) ||
           user.role === 'beta_tester' ||
           user.organization.id in BETA_ORGS;
}
```

### Implementation

#### Simple Boolean Flags

```python
# config.py
FEATURE_FLAGS = {
    'new_search_algorithm': False,
    'new_user_profile': True,
}

# usage
if FEATURE_FLAGS['new_search_algorithm']:
    results = new_search(query)
else:
    results = old_search(query)
```

#### Percentage-Based Rollout

```python
class FeatureFlags:
    def is_enabled(self, flag_name, user_id=None):
        config = self.get_flag_config(flag_name)

        if not config.enabled:
            return False

        if user_id:
            # Consistent hashing for user
            user_hash = hash(f"{flag_name}:{user_id}") % 100
            return user_hash < config.percentage

        return True

# Usage
if feature_flags.is_enabled('new_checkout', user.id):
    # 30% of users see new checkout
    render_new_checkout()
else:
    render_old_checkout()
```

#### External Feature Flag Service

```python
# Using service like LaunchDarkly, Split.io, etc.
from ldclient import get as get_ld_client

ld_client = get_ld_client()

def should_use_new_feature(user):
    return ld_client.variation('new-feature-key', user, False)
```

### Best Practices

1. **Short-Lived Toggles**: Remove after migration complete
2. **Clear Naming**: Name indicates purpose and timeline
3. **Documentation**: Document what each toggle controls
4. **Toggle Inventory**: Track all active toggles
5. **Cleanup**: Schedule toggle removal
6. **Testing**: Test both code paths

### Migration Timeline with Toggles

```
Week 0:  Implement new feature behind toggle (default: off)
Week 1:  Enable for internal users (1%)
Week 2:  Enable for beta users (10%)
Week 3:  Gradual rollout (50%)
Week 4:  Full rollout (100%)
Week 5:  Monitor, verify success
Week 6:  Remove toggle, delete old code
```

---

## Branch by Abstraction

### Concept

Make large-scale changes without breaking the build by:
1. Creating abstraction over current implementation
2. Replacing implementation behind abstraction
3. Removing abstraction once complete

### Implementation Steps

#### Step 1: Introduce Abstraction

```java
// Current code
class OrderProcessor {
    public void process(Order order) {
        // Direct use of legacy payment gateway
        LegacyPaymentGateway.charge(order.total);
    }
}

// Step 1: Introduce abstraction
interface PaymentGateway {
    void charge(BigDecimal amount);
}

class LegacyPaymentGatewayAdapter implements PaymentGateway {
    public void charge(BigDecimal amount) {
        LegacyPaymentGateway.charge(amount);
    }
}
```

#### Step 2: Update Usage to Use Abstraction

```java
class OrderProcessor {
    private PaymentGateway paymentGateway;

    public OrderProcessor() {
        this.paymentGateway = new LegacyPaymentGatewayAdapter();
    }

    public void process(Order order) {
        paymentGateway.charge(order.total);
    }
}
```

#### Step 3: Implement New Version

```java
class ModernPaymentGateway implements PaymentGateway {
    public void charge(BigDecimal amount) {
        // New implementation
        StripeAPI.createCharge(amount);
    }
}
```

#### Step 4: Switch Implementation

```java
class OrderProcessor {
    private PaymentGateway paymentGateway;

    public OrderProcessor() {
        // Switch to new implementation
        this.paymentGateway = new ModernPaymentGateway();
    }

    public void process(Order order) {
        paymentGateway.charge(order.total);
    }
}
```

#### Step 5: Remove Abstraction (Optional)

Once migration complete and proven:

```java
class OrderProcessor {
    private ModernPaymentGateway paymentGateway;

    public void process(Order order) {
        // Direct usage again, but with new implementation
        paymentGateway.charge(order.total);
    }
}

// Delete PaymentGateway interface
// Delete LegacyPaymentGatewayAdapter
```

### Benefits

- Build never breaks
- Changes are incremental
- Easy to rollback
- Team can continue working

### When to Use

- Replacing core dependencies
- Migrating to new library/framework
- Large refactorings affecting many files
- Need to maintain working build throughout

---

## Parallel Run Pattern

### Concept

Run old and new implementations simultaneously and compare results.

### Implementation

```python
class ParallelRunner:
    def __init__(self, old_impl, new_impl):
        self.old = old_impl
        self.new = new_impl
        self.metrics = MetricsCollector()

    def execute(self, input_data):
        # Always run old implementation (primary)
        old_result = self.old.execute(input_data)

        # Run new implementation in parallel (shadow)
        try:
            new_result = self.new.execute(input_data)

            # Compare results
            if self.results_match(old_result, new_result):
                self.metrics.record('match')
            else:
                self.metrics.record('mismatch')
                self.log_difference(input_data, old_result, new_result)
        except Exception as e:
            self.metrics.record('new_impl_error')
            self.log_error(input_data, e)

        # Always return old result (safe)
        return old_result

    def results_match(self, old, new):
        # Define what "match" means for your use case
        return old == new
```

### Dark Launch

Run new code in production but don't use results:

```javascript
async function searchProducts(query) {
    // Primary path (old implementation)
    const oldResults = await oldSearch(query);

    // Dark launch new search (don't block, don't fail)
    newSearch(query)
        .then(newResults => {
            compareResults(oldResults, newResults);
            metrics.record('new_search_completed');
        })
        .catch(error => {
            metrics.record('new_search_failed', error);
        });

    // Return old results immediately
    return oldResults;
}
```

### Benefits

- Validate new implementation with real data
- No user impact if new version fails
- Build confidence before switching
- Discover edge cases

### Challenges

- Increased resource usage
- Potential side effects from running twice
- Need metrics to compare results

### When to Use

- High-risk changes
- Complex algorithms
- Unproven new implementation
- Need data to build confidence

---

## Database Migration Strategies

### Expand-Contract Pattern

Three-phase approach for database changes:

#### Phase 1: Expand

Add new schema alongside old:

```sql
-- Current schema
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

-- Phase 1: Add new column
ALTER TABLE users ADD COLUMN first_name VARCHAR(255);
ALTER TABLE users ADD COLUMN last_name VARCHAR(255);
```

Application writes to both old and new:

```python
def update_user(user_id, full_name):
    # Write to old column
    db.execute(
        "UPDATE users SET name = ? WHERE id = ?",
        [full_name, user_id]
    )

    # Also write to new columns
    first, last = split_name(full_name)
    db.execute(
        "UPDATE users SET first_name = ?, last_name = ? WHERE id = ?",
        [first, last, user_id]
    )
```

#### Phase 2: Migrate

Backfill new columns with data from old:

```sql
-- Migrate existing data
UPDATE users
SET first_name = SUBSTRING_INDEX(name, ' ', 1),
    last_name = SUBSTRING_INDEX(name, ' ', -1)
WHERE first_name IS NULL;
```

#### Phase 3: Contract

Remove old schema:

```python
# Update application to only use new columns
def update_user(user_id, first_name, last_name):
    db.execute(
        "UPDATE users SET first_name = ?, last_name = ? WHERE id = ?",
        [first_name, last_name, user_id]
    )
```

```sql
-- Remove old column
ALTER TABLE users DROP COLUMN name;
```

### Transitional Database States

Ensure database works with multiple application versions:

```
DB V1  ← → App V1 (old)
  ↓
DB V2  ← → App V1 (old) + App V2 (new)
  ↓
DB V3  ← → App V2 (new)
```

Each DB version must support apps before and after.

---

## API Versioning and Migration

### URL Versioning

```
/api/v1/users
/api/v2/users
```

**Pros**: Clear, explicit
**Cons**: Version proliferation

### Header Versioning

```http
GET /api/users
Accept: application/vnd.myapp.v2+json
```

**Pros**: Clean URLs
**Cons**: Less visible

### Gradual Migration Strategy

```python
# Support both versions
@app.route('/api/users/<user_id>')
def get_user(user_id):
    version = request.headers.get('API-Version', '1')

    user = User.find(user_id)

    if version == '2':
        return serialize_v2(user)
    else:
        return serialize_v1(user)

def serialize_v1(user):
    return {
        'id': user.id,
        'name': user.full_name,
        'email': user.email
    }

def serialize_v2(user):
    return {
        'id': user.id,
        'firstName': user.first_name,
        'lastName': user.last_name,
        'email': user.email,
        'createdAt': user.created_at.isoformat()
    }
```

### Deprecation Process

1. **Announce**: Communicate deprecation timeline
2. **Monitor**: Track usage of old version
3. **Migrate**: Help clients migrate
4. **Sunset**: Remove old version

```python
@app.route('/api/v1/users')
def get_users_v1():
    # Add deprecation warning
    response = make_response(serialize_users_v1())
    response.headers['Warning'] = '299 - "API v1 is deprecated. Please migrate to v2 by 2024-12-31"'
    response.headers['Sunset'] = 'Wed, 31 Dec 2024 23:59:59 GMT'

    # Log usage for monitoring
    metrics.increment('api.v1.usage')

    return response
```

---

## Conclusion

Successful migration is about:

- **Incrementalism**: Small, safe steps
- **Coexistence**: Old and new work together
- **Validation**: Prove new system works
- **Reversibility**: Can rollback if needed
- **Patience**: Takes time, don't rush

**Key Principle**: Never put yourself in a position where you can't deploy.

Choose your pattern based on:
- Risk level
- System complexity
- Team capacity
- Business constraints

*Remember: Migration is a marathon, not a sprint.*
