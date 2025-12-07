# Code Review Checklist

A comprehensive checklist for conducting thorough code reviews. Use this as a guide to ensure no critical aspects are overlooked.

## 1. Functionality and Logic

### Core Functionality
- [ ] Code implements the stated requirements
- [ ] Logic is correct and produces expected results
- [ ] All use cases are handled appropriately
- [ ] Edge cases are considered and handled
- [ ] Boundary conditions are tested
- [ ] Error conditions are properly handled

### Business Logic
- [ ] Business rules are correctly implemented
- [ ] Data validation is appropriate
- [ ] Calculations and algorithms are correct
- [ ] State transitions are valid
- [ ] Workflow logic is sound

### Error Handling
- [ ] All potential errors are caught
- [ ] Error messages are clear and actionable
- [ ] Errors are logged appropriately
- [ ] No errors are silently swallowed
- [ ] Recovery mechanisms are in place
- [ ] User-facing errors are user-friendly

## 2. Security

### Input Validation
- [ ] All user inputs are validated
- [ ] Input sanitization prevents injection attacks
- [ ] File uploads are restricted and validated
- [ ] URL parameters are validated
- [ ] Request size limits are enforced

### Authentication & Authorization
- [ ] Authentication is required where needed
- [ ] Authorization checks are present
- [ ] Session management is secure
- [ ] Password handling follows best practices
- [ ] Token management is secure
- [ ] Role-based access control is correct

### Data Protection
- [ ] Sensitive data is encrypted at rest
- [ ] Sensitive data is encrypted in transit (HTTPS)
- [ ] No secrets in code or logs
- [ ] Personal data handling complies with regulations
- [ ] Database queries use parameterization
- [ ] No SQL injection vulnerabilities

### Common Vulnerabilities
- [ ] No XSS vulnerabilities
- [ ] No CSRF vulnerabilities
- [ ] No insecure direct object references
- [ ] No unvalidated redirects
- [ ] No security misconfiguration
- [ ] Dependencies have no known vulnerabilities

## 3. Performance

### Algorithm Efficiency
- [ ] Algorithms have appropriate time complexity
- [ ] No unnecessary nested loops
- [ ] Data structures are appropriate
- [ ] Caching is used where beneficial
- [ ] Lazy loading is implemented where appropriate

### Database
- [ ] Queries are optimized
- [ ] No N+1 query problems
- [ ] Appropriate indexes are used
- [ ] Connection pooling is configured
- [ ] Transactions are used appropriately
- [ ] Batch operations where possible

### Resource Management
- [ ] Files are closed after use
- [ ] Database connections are closed
- [ ] Memory leaks are prevented
- [ ] Large datasets are processed in chunks
- [ ] Resource limits are respected

### Scalability
- [ ] Code scales with increased load
- [ ] No hard-coded limits that prevent scaling
- [ ] Stateless where possible
- [ ] Asynchronous processing for long operations

## 4. Code Quality

### Readability
- [ ] Code is easy to understand
- [ ] Variable names are descriptive
- [ ] Function names clearly describe purpose
- [ ] Class names follow conventions
- [ ] Code is properly formatted
- [ ] Consistent coding style

### Maintainability
- [ ] Code follows DRY principle
- [ ] Functions are single-purpose
- [ ] Classes have clear responsibilities
- [ ] Code is modular
- [ ] Dependencies are minimal
- [ ] No code duplication

### Complexity
- [ ] Functions are reasonably short (<50 lines)
- [ ] Cyclomatic complexity is low (<10)
- [ ] Nesting depth is reasonable (<4 levels)
- [ ] Number of parameters is reasonable (<5)
- [ ] Cognitive complexity is low

### Comments and Documentation
- [ ] Complex logic has explanatory comments
- [ ] Comments explain "why", not "what"
- [ ] Public APIs are documented
- [ ] Documentation is up-to-date
- [ ] No commented-out code
- [ ] TODO comments have owner and date

## 5. Testing

### Test Coverage
- [ ] New code has unit tests
- [ ] Edge cases are tested
- [ ] Error conditions are tested
- [ ] Integration tests exist where needed
- [ ] Test coverage meets project standards
- [ ] Critical paths are well-tested

### Test Quality
- [ ] Tests are independent
- [ ] Tests are repeatable
- [ ] Tests are clear and understandable
- [ ] Test names describe what they test
- [ ] Mocks and stubs are used appropriately
- [ ] No flaky tests

### Test Types
- [ ] Unit tests for isolated components
- [ ] Integration tests for component interaction
- [ ] E2E tests for critical workflows
- [ ] Performance tests where applicable
- [ ] Security tests where applicable

## 6. Architecture and Design

### Design Principles
- [ ] Single Responsibility Principle followed
- [ ] Open/Closed Principle followed
- [ ] Liskov Substitution Principle followed
- [ ] Interface Segregation Principle followed
- [ ] Dependency Inversion Principle followed
- [ ] KISS principle applied
- [ ] YAGNI principle applied

### Code Organization
- [ ] Files are properly organized
- [ ] Modules have clear boundaries
- [ ] Separation of concerns is maintained
- [ ] Layering is appropriate
- [ ] Dependencies flow in correct direction

### Design Patterns
- [ ] Appropriate design patterns are used
- [ ] Patterns are not misapplied
- [ ] No anti-patterns present
- [ ] Singleton pattern used judiciously
- [ ] Factory patterns used appropriately

### Coupling and Cohesion
- [ ] Low coupling between modules
- [ ] High cohesion within modules
- [ ] No circular dependencies
- [ ] Dependencies are explicit
- [ ] Interfaces over implementations

## 7. API and Interface Design

### API Design
- [ ] API is intuitive and easy to use
- [ ] Consistent naming conventions
- [ ] Appropriate HTTP methods (REST)
- [ ] Proper status codes returned
- [ ] Versioning strategy is clear
- [ ] Backward compatibility maintained

### Input/Output
- [ ] Input validation is comprehensive
- [ ] Output format is consistent
- [ ] Error responses are standardized
- [ ] Pagination for large datasets
- [ ] Rate limiting where appropriate

### Documentation
- [ ] API endpoints are documented
- [ ] Request/response examples provided
- [ ] Error codes documented
- [ ] Authentication requirements clear
- [ ] Usage examples provided

## 8. Configuration and Environment

### Configuration Management
- [ ] No hardcoded configuration
- [ ] Environment-specific configs separated
- [ ] Secrets managed securely
- [ ] Configuration validation on startup
- [ ] Default values are sensible

### Dependencies
- [ ] All dependencies are necessary
- [ ] Dependency versions are pinned
- [ ] No deprecated dependencies
- [ ] License compatibility checked
- [ ] Transitive dependencies reviewed

## 9. Logging and Monitoring

### Logging
- [ ] Appropriate log levels used
- [ ] Sensitive data not logged
- [ ] Logs are structured
- [ ] Log messages are clear
- [ ] Errors are logged with context
- [ ] Performance impacts considered

### Monitoring
- [ ] Key metrics are tracked
- [ ] Alerts are configured appropriately
- [ ] Debugging information available
- [ ] Performance metrics collected
- [ ] Business metrics tracked

## 10. Compatibility and Integration

### Backward Compatibility
- [ ] Breaking changes are documented
- [ ] Migration path is provided
- [ ] Deprecated features are marked
- [ ] Version compatibility maintained

### Integration
- [ ] External service failures handled
- [ ] API contracts are respected
- [ ] Integration points are tested
- [ ] Timeouts are configured
- [ ] Retries are implemented appropriately

## 11. User Experience

### Functionality
- [ ] Features work as expected
- [ ] UI is intuitive
- [ ] Error messages are helpful
- [ ] Loading states are shown
- [ ] Success feedback is provided

### Accessibility
- [ ] Keyboard navigation works
- [ ] Screen reader compatible
- [ ] Color contrast is sufficient
- [ ] ARIA labels are present
- [ ] Focus management is correct

### Performance
- [ ] Page load time is acceptable
- [ ] No blocking operations in UI
- [ ] Smooth animations
- [ ] Responsive design works
- [ ] Optimistic updates where appropriate

## 12. Database and Data

### Schema
- [ ] Schema changes are versioned
- [ ] Migrations are reversible
- [ ] Indexes are appropriate
- [ ] Constraints are defined
- [ ] Data types are appropriate

### Data Integrity
- [ ] Transactions maintain consistency
- [ ] Referential integrity maintained
- [ ] Data validation at database level
- [ ] Concurrency handled properly
- [ ] Orphaned records prevented

### Data Access
- [ ] ORM used correctly
- [ ] Raw queries are justified
- [ ] Prepared statements used
- [ ] Connection pooling configured
- [ ] Query timeouts set

## 13. DevOps and Deployment

### Build and Deploy
- [ ] Build process is reproducible
- [ ] Deployment process is documented
- [ ] Rollback strategy exists
- [ ] Feature flags used appropriately
- [ ] Zero-downtime deployment possible

### CI/CD
- [ ] All tests pass in CI
- [ ] Linters pass
- [ ] Security scans pass
- [ ] Build artifacts are versioned
- [ ] Deployment is automated

## 14. Documentation

### Code Documentation
- [ ] README is updated
- [ ] API documentation updated
- [ ] Architecture diagrams updated
- [ ] Configuration documented
- [ ] Deployment guide updated

### Change Documentation
- [ ] CHANGELOG updated
- [ ] Breaking changes highlighted
- [ ] Migration guide provided
- [ ] Release notes prepared

## Review Completion

Before approving:
- [ ] All critical issues addressed
- [ ] Major issues resolved or discussed
- [ ] Tests pass locally and in CI
- [ ] Documentation is complete
- [ ] No unresolved questions
- [ ] Code meets team standards

## Notes
- Not all items apply to every change
- Use judgment to determine relevance
- Focus on items appropriate to change size
- Consider project-specific requirements
- Prioritize critical issues over minor ones
