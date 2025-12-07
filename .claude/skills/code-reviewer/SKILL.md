---
name: code-reviewer
description: Comprehensive code review skill for detecting bugs, vulnerabilities, complexity issues, and suggesting improvements based on clean code principles
---

# Code Reviewer Skill

A thorough code review workflow that analyzes code quality, detects potential issues, and provides actionable feedback based on industry best practices.

## Core Capabilities

### 1. Bug and Vulnerability Detection
- **Logic Errors**: Identify incorrect conditional logic, off-by-one errors, null pointer dereferences
- **Security Vulnerabilities**: SQL injection, XSS, insecure authentication, exposed secrets
- **Resource Leaks**: Unclosed file handles, database connections, memory leaks
- **Race Conditions**: Thread safety issues, synchronization problems
- **Error Handling**: Missing try-catch blocks, swallowed exceptions, improper error propagation

### 2. Complexity Analysis
- **Cyclomatic Complexity**: Flag functions with too many decision points (threshold: >10)
- **Cognitive Complexity**: Assess mental overhead required to understand code
- **Nesting Depth**: Identify deeply nested code blocks (threshold: >4 levels)
- **Function Length**: Flag overly long functions (threshold: >50 lines)
- **Parameter Count**: Flag functions with too many parameters (threshold: >5)

### 3. Code Quality Assessment
- **Naming Conventions**: Variables, functions, classes follow language standards
- **Code Duplication**: Identify repeated code blocks that should be extracted
- **Magic Numbers**: Hardcoded values that should be named constants
- **Dead Code**: Unused variables, functions, imports
- **Code Smells**: Refer to `/references/code-smells.md` for comprehensive catalog

### 4. Architecture and Design
- **SOLID Principles**: Single Responsibility, Open/Closed, Liskov Substitution, Interface Segregation, Dependency Inversion
- **Design Patterns**: Proper application of patterns, anti-pattern detection
- **Separation of Concerns**: Proper layering, business logic separation
- **Dependency Management**: Circular dependencies, tight coupling issues

### 5. Best Practices
- **DRY (Don't Repeat Yourself)**: Minimize code duplication
- **KISS (Keep It Simple, Stupid)**: Avoid over-engineering
- **YAGNI (You Aren't Gonna Need It)**: No premature optimization
- **Performance**: Inefficient algorithms, N+1 queries, unnecessary computations
- **Testing**: Test coverage, testability of code

## Review Workflow

### Phase 1: Initial Assessment
1. **Understand Context**: Review PR description, related issues, and purpose
2. **Scope Analysis**: Identify files changed, lines added/removed
3. **Impact Assessment**: Determine affected components and systems

### Phase 2: Automated Analysis
1. **Static Analysis**: Run linters, formatters, security scanners
2. **Complexity Metrics**: Calculate cyclomatic and cognitive complexity
3. **Test Coverage**: Check if new code is adequately tested
4. **Dependency Check**: Verify no vulnerable dependencies added

### Phase 3: Manual Review
1. **Code Reading**: Read through all changes line by line
2. **Logic Verification**: Verify correctness of implementation
3. **Edge Cases**: Consider boundary conditions and error scenarios
4. **Security Review**: Check for common security vulnerabilities
5. **Performance Review**: Identify potential performance bottlenecks

### Phase 4: Feedback Generation
1. **Categorize Issues**: Critical / Major / Minor priority
2. **Provide Context**: Explain why something is an issue
3. **Suggest Solutions**: Offer concrete improvement suggestions
4. **Use Templates**: Refer to `/references/review-comment-templates.md`

### Phase 5: Review Summary
1. **Overall Assessment**: High-level summary of code quality
2. **Key Issues**: Highlight most important problems
3. **Positive Feedback**: Acknowledge good practices
4. **Action Items**: Clear next steps for author

## Priority Classification

### Critical (Must Fix Before Merge)
- Security vulnerabilities
- Data corruption risks
- Breaking changes without migration
- Severe performance degradation
- Complete test failures

### Major (Should Fix Before Merge)
- Logic errors affecting core functionality
- Poor error handling
- Significant code smells
- Missing important tests
- High complexity issues

### Minor (Nice to Have)
- Naming inconsistencies
- Minor code duplication
- Documentation improvements
- Code style issues
- Optimization opportunities

## Review Checklist

Before completing a review, ensure:
- [ ] All code changes are understood
- [ ] Logic is correct and handles edge cases
- [ ] No security vulnerabilities introduced
- [ ] Error handling is appropriate
- [ ] Code is readable and maintainable
- [ ] Tests are adequate and passing
- [ ] Performance implications considered
- [ ] Documentation is updated
- [ ] Follows project coding standards
- [ ] No breaking changes without discussion

Refer to `/references/review-checklist.md` for detailed checklist.

## Usage

When reviewing code:

1. **Start with Overview**: Understand the big picture
2. **Use References**: Leverage checklists and templates in `/references/`
3. **Be Constructive**: Focus on improvement, not criticism
4. **Be Specific**: Point to exact lines and suggest concrete changes
5. **Educate**: Explain the "why" behind suggestions
6. **Acknowledge Good Work**: Recognize well-written code

## Output Format

Structure review comments as:

```
**[Priority Level]** [Issue Type]: [Brief Description]

[Detailed explanation of the issue]

Suggestion:
[Concrete improvement recommendation with code example if applicable]

Reference: [Link to relevant principle/pattern if applicable]
```

Example:
```
**Major** Complexity Issue: Function has high cyclomatic complexity (15)

The `processUserData` function has too many nested conditions and decision points, making it difficult to understand and test.

Suggestion:
Extract validation logic into separate functions:
- `validateUserInput()`
- `sanitizeUserData()`
- `processValidatedData()`

Reference: Clean Code Principles - Single Responsibility Principle
```

## Resources

- `/references/review-checklist.md`: Comprehensive review checklist
- `/references/code-smells.md`: Catalog of code smells with detection strategies
- `/references/clean-code-principles.md`: SOLID, DRY, KISS, YAGNI principles
- `/references/review-comment-templates.md`: Ready-to-use comment templates

## Best Practices for Reviewers

1. **Review Promptly**: Don't let PRs sit for days
2. **Be Respectful**: Assume positive intent
3. **Ask Questions**: "Can you explain why..." instead of "This is wrong"
4. **Provide Context**: Explain your reasoning
5. **Consider Trade-offs**: Perfect code doesn't exist
6. **Limit Scope**: Don't expand review beyond PR scope
7. **Use Automation**: Let tools catch style issues
8. **Follow Up**: Check if feedback was addressed

## Integration with Development Workflow

This skill integrates with:
- **Pre-commit hooks**: Automated checks before commit
- **CI/CD pipelines**: Automated reviews in pipeline
- **Pull request templates**: Structured PR descriptions
- **Issue tracking**: Link reviews to issues
- **Documentation**: Update docs based on review findings
