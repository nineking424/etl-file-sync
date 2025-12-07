---
name: refactorer
description: |
  Improve code structure and quality through systematic refactoring.
  Use when: (1) Reducing code complexity, (2) Eliminating duplication,
  (3) Improving readability, (4) Applying design patterns,
  (5) Breaking down large classes/methods, (6) Modernizing legacy code.
---

# Refactorer Skill

## Purpose

The Refactorer skill guides systematic code improvement through proven refactoring techniques. It helps transform existing code into cleaner, more maintainable, and more extensible structures while preserving functionality through comprehensive testing.

## When to Use This Skill

Activate this skill when you need to:

1. **Reduce Code Complexity**: Simplify convoluted logic, reduce cyclomatic complexity
2. **Eliminate Duplication**: Apply DRY principles, extract common patterns
3. **Improve Readability**: Enhance code clarity, naming, and organization
4. **Apply Design Patterns**: Introduce or refactor to design patterns
5. **Break Down Large Units**: Split large classes, methods, or modules
6. **Modernize Legacy Code**: Update old code to modern standards and practices
7. **Prepare for Feature Addition**: Restructure code to accommodate new features
8. **Fix Code Smells**: Address technical debt and anti-patterns

## Core Capabilities

### 1. Martin Fowler Refactoring Catalog Application

Apply systematic refactoring patterns from the authoritative catalog:

- **Method Refactorings**: Extract Method, Inline Method, Extract Variable, Inline Variable
- **Class Refactorings**: Extract Class, Inline Class, Hide Delegate, Remove Middle Man
- **Data Refactorings**: Encapsulate Field, Replace Data Value with Object
- **Conditional Refactorings**: Decompose Conditional, Consolidate Conditional Expression
- **API Refactorings**: Rename Method, Add Parameter, Remove Parameter, Parameterize Method

### 2. Incremental Refactoring Planning

Break down large refactoring efforts into safe, manageable steps:

- **Impact Analysis**: Identify all affected code areas
- **Dependency Mapping**: Understand relationships and dependencies
- **Staged Execution**: Plan refactoring in testable increments
- **Milestone Checkpoints**: Define verification points between stages
- **Rollback Strategy**: Maintain ability to revert safely

### 3. Test Preservation Strategy

Ensure functionality remains intact throughout refactoring:

- **Pre-Refactoring Test Suite**: Verify comprehensive test coverage
- **Red-Green-Refactor**: Follow TDD cycle for safety
- **Test-First Refactoring**: Add missing tests before structural changes
- **Continuous Verification**: Run tests after each incremental change
- **Regression Prevention**: Maintain or improve test coverage

### 4. Impact Scope Analysis

Assess and communicate refactoring implications:

- **Code Change Scope**: Identify all files and modules affected
- **API Compatibility**: Determine breaking vs. non-breaking changes
- **Performance Impact**: Evaluate potential performance implications
- **Risk Assessment**: Categorize changes by risk level (low/medium/high)
- **Stakeholder Communication**: Provide clear before/after comparisons

## Workflow

### Phase 1: Assessment

1. **Identify Code Smell**: Recognize the problem pattern
   - Long Method, Large Class, Duplicated Code
   - Feature Envy, Data Clumps, Primitive Obsession
   - Long Parameter List, Divergent Change, Shotgun Surgery

2. **Analyze Current State**:
   - Review existing tests
   - Map dependencies
   - Identify coupling points
   - Assess complexity metrics

3. **Define Success Criteria**:
   - Specific improvements expected
   - Metrics to track (complexity, duplication, coverage)
   - Performance benchmarks if relevant

### Phase 2: Planning

1. **Select Refactoring Pattern(s)**:
   - Choose appropriate catalog entries
   - Consider pattern combinations
   - Plan application sequence

2. **Create Incremental Plan**:
   - Break into small, testable steps
   - Define checkpoint milestones
   - Identify potential risks per step

3. **Prepare Test Suite**:
   - Add missing test coverage
   - Create characterization tests for legacy code
   - Establish baseline performance tests if needed

### Phase 3: Execution

1. **Apply Refactoring Incrementally**:
   - Make one change at a time
   - Run tests after each change
   - Commit frequently with descriptive messages

2. **Monitor Impact**:
   - Track complexity metrics
   - Verify test coverage maintenance
   - Check performance benchmarks

3. **Document Changes**:
   - Update code comments where necessary
   - Revise API documentation
   - Note architectural decisions

### Phase 4: Verification

1. **Comprehensive Testing**:
   - Full test suite execution
   - Manual testing of critical paths
   - Integration test verification

2. **Code Review**:
   - Peer review of changes
   - Verify readability improvements
   - Confirm pattern application

3. **Performance Validation**:
   - Compare benchmarks
   - Profile if concerns exist
   - Optimize if regressions found

## Refactoring Safety Guidelines

### Always Safe

- Rename (with IDE refactoring tools)
- Extract Method/Function
- Extract Variable/Constant
- Inline Temp
- Replace Magic Number with Named Constant

### Requires Careful Testing

- Move Method/Field
- Change Method Signature
- Extract Class
- Replace Conditional with Polymorphism
- Replace Type Code with State/Strategy

### High Risk - Proceed with Caution

- Change Bidirectional Association to Unidirectional
- Replace Inheritance with Delegation
- Replace Delegation with Inheritance
- Tease Apart Inheritance
- Large-scale architectural changes

## Common Refactoring Scenarios

### Scenario 1: Long Method

**Code Smell**: Method exceeds 20-30 lines, does multiple things

**Refactoring Approach**:
1. Extract Method for each logical section
2. Apply Extract Variable for complex expressions
3. Use Replace Temp with Query where appropriate
4. Consider Extract Class if method operates on subset of data

### Scenario 2: Duplicated Code

**Code Smell**: Similar code appears in multiple locations

**Refactoring Approach**:
1. Extract Method for identical code blocks
2. Pull Up Method if duplication in subclasses
3. Form Template Method if similar structure, different details
4. Extract Class if duplication spans multiple methods

### Scenario 3: Large Class

**Code Smell**: Class has too many responsibilities (God Object)

**Refactoring Approach**:
1. Identify distinct responsibilities
2. Extract Class for each responsibility
3. Apply Extract Interface if needed
4. Use Extract Subclass for variant behavior

### Scenario 4: Primitive Obsession

**Code Smell**: Using primitives instead of small objects

**Refactoring Approach**:
1. Replace Data Value with Object
2. Introduce Parameter Object for grouped primitives
3. Replace Type Code with Class
4. Replace Type Code with Subclasses/State if behavior varies

### Scenario 5: Long Parameter List

**Code Smell**: Method has more than 3-4 parameters

**Refactoring Approach**:
1. Replace Parameter with Method Call
2. Preserve Whole Object instead of extracting values
3. Introduce Parameter Object for grouped parameters
4. Replace Parameter with Explicit Methods for flag parameters

## Best Practices

### Before Starting

- Ensure all existing tests pass
- Create a feature branch
- Communicate refactoring plans to team
- Establish time-box for refactoring effort

### During Refactoring

- Follow the Boy Scout Rule: leave code better than you found it
- Keep changes focused and incremental
- Commit after each successful refactoring step
- Run the full test suite frequently
- Don't add features while refactoring
- Don't change behavior while refactoring

### After Refactoring

- Verify all tests still pass
- Review metrics improvements
- Update documentation
- Request code review
- Share learnings with team

## Integration with Development Workflow

### Opportunistic Refactoring

- Refactor as you encounter code smells during feature work
- Apply the "15-minute rule": if refactoring takes < 15 minutes, do it now
- Make small improvements continuously

### Dedicated Refactoring

- Schedule refactoring sprints for major technical debt
- Address high-priority code smells systematically
- Use metrics to identify refactoring candidates

### Pre-Feature Refactoring

- Refactor existing code before adding new features
- Create appropriate extension points
- Simplify the area where new code will integrate

## Success Metrics

Track these indicators to measure refactoring effectiveness:

- **Complexity Reduction**: Lower cyclomatic complexity scores
- **Duplication Elimination**: Reduced code duplication percentage
- **Test Coverage**: Maintained or improved coverage
- **Code Comprehension**: Faster understanding in code reviews
- **Change Velocity**: Easier/faster feature additions post-refactoring
- **Bug Reduction**: Fewer defects in refactored areas

## References

See the `references/` directory for detailed information:

- `refactoring-catalog.md`: Comprehensive refactoring pattern catalog
- `safe-refactoring.md`: Step-by-step safe refactoring procedures
- `migration-patterns.md`: Strategies for legacy code modernization

## Examples

Before/after refactoring examples can be found in `assets/before-after-examples/`

## Warnings and Limitations

- **Performance**: Some refactorings may introduce minor performance overhead; profile if critical
- **Breaking Changes**: API refactorings may require coordinated updates across codebase
- **Over-Engineering**: Avoid premature abstraction; refactor based on actual needs
- **Time Investment**: Large refactorings require significant time; balance with feature delivery
- **Team Alignment**: Ensure team understands and agrees with refactoring approaches

## Related Skills

- **code-reviewer**: For reviewing refactored code
- **test-writer**: For creating tests before refactoring
- **performance-optimizer**: When refactoring impacts performance
- **architecture-advisor**: For large-scale structural refactorings

---

*Remember: The goal of refactoring is not perfection, but continuous improvement toward more maintainable, understandable, and extensible code.*
