---
name: document-maintainer
description: Comprehensive technical documentation maintenance workflow - auto-update docs on code changes, generate Javadoc/Docstrings, create/update Mermaid diagrams, and maintain API specifications (OpenAPI/Swagger)
---

# Document Maintainer

A comprehensive workflow for maintaining high-quality technical documentation across your codebase. This skill helps you keep documentation synchronized with code changes, generate API documentation, and create visual diagrams.

## Core Capabilities

### 1. Auto-Update Documentation on Code Changes

When code changes are detected, automatically:
- Update affected README files
- Sync API documentation with code
- Refresh code examples in documentation
- Update version numbers and changelogs
- Flag outdated documentation sections

**Workflow:**
1. Detect code changes (git diff, file modifications)
2. Identify affected documentation files
3. Parse code structure (functions, classes, interfaces)
4. Update documentation sections
5. Validate links and references
6. Generate change summary

### 2. Javadoc/Docstring Generation

Generate comprehensive inline documentation:
- **Java**: Javadoc comments with @param, @return, @throws
- **Python**: Docstrings (Google, NumPy, Sphinx styles)
- **JavaScript/TypeScript**: JSDoc comments
- **C#**: XML documentation comments
- **Go**: Go doc comments

**Best Practices:**
- Include parameter descriptions and types
- Document return values and exceptions
- Add usage examples for complex functions
- Reference related functions/classes
- Include since/deprecated tags when applicable

### 3. Mermaid Diagram Generation/Update

Create and maintain visual documentation:
- **Flowcharts**: Process flows, decision trees
- **Sequence Diagrams**: API interactions, request flows
- **Class Diagrams**: Object-oriented architecture
- **ER Diagrams**: Database schemas
- **State Diagrams**: Application states
- **Gantt Charts**: Project timelines

**Update Triggers:**
- New classes/modules added
- API endpoints changed
- Database schema modified
- Process flows updated

### 4. API Spec Documentation (OpenAPI)

Maintain OpenAPI/Swagger specifications:
- Generate OpenAPI 3.0+ specs from code
- Document endpoints, parameters, responses
- Include example requests/responses
- Add authentication/authorization details
- Generate interactive API documentation

## Workflow Steps

### Step 1: Documentation Audit
```bash
# Identify documentation files
find . -name "*.md" -o -name "*.adoc" -o -name "*.rst"

# Check for missing documentation
# - Undocumented functions/classes
# - Missing README files
# - Outdated API specs
```

### Step 2: Code Analysis
- Parse source code for changes
- Extract function/class signatures
- Identify API endpoints
- Map database models

### Step 3: Documentation Generation
- Generate/update inline comments
- Create/refresh markdown documentation
- Update Mermaid diagrams
- Sync OpenAPI specifications

### Step 4: Validation
- Check for broken links
- Validate code examples
- Ensure consistent formatting
- Verify diagram syntax

### Step 5: Review and Commit
- Present documentation changes
- Request user review
- Commit documentation updates
- Update documentation version

## Usage Examples

### Example 1: Update README After Code Changes
```
User: I've added new authentication endpoints. Update the documentation.

Claude:
1. Analyzes new endpoints in auth.controller.ts
2. Updates README.md API section
3. Generates OpenAPI spec for auth endpoints
4. Creates sequence diagram for OAuth flow
5. Commits: "docs: update authentication documentation"
```

### Example 2: Generate Javadoc for New Class
```
User: Add Javadoc to the UserService class.

Claude:
1. Analyzes UserService methods
2. Generates comprehensive Javadoc comments
3. Includes @param, @return, @throws tags
4. Adds usage examples
5. Documents exception handling
```

### Example 3: Create Architecture Diagram
```
User: Create a Mermaid diagram showing the microservices architecture.

Claude:
1. Analyzes service structure
2. Identifies service dependencies
3. Generates C4 or component diagram
4. Adds to architecture.md
5. Includes diagram in README
```

## Integration Points

### Git Integration
- Pre-commit hooks for documentation validation
- Automated doc updates on merge
- Documentation versioning with code

### CI/CD Integration
- Documentation build validation
- API spec compatibility checks
- Diagram syntax validation
- Link checking in pipelines

### IDE Integration
- Real-time documentation linting
- Inline documentation suggestions
- Diagram preview
- API spec validation

## Configuration

### Documentation Standards
Refer to `references/documentation-standards.md` for:
- Writing style guidelines
- Formatting conventions
- Code example standards
- Diagram best practices

### API Documentation
Refer to `references/api-doc-patterns.md` for:
- OpenAPI specification patterns
- Endpoint documentation templates
- Request/response examples
- Authentication documentation

### Diagram Conventions
Refer to `references/diagram-conventions.md` for:
- Mermaid diagram types
- Styling guidelines
- Naming conventions
- Common patterns

## Templates

Use templates from `assets/templates/` for consistent documentation:
- **readme-template.md**: Standard README structure
- **api-doc-template.md**: API endpoint documentation
- **architecture-doc-template.md**: System architecture docs

## Best Practices

1. **Keep Documentation Close to Code**
   - Inline comments for implementation details
   - README in each module/package
   - Architecture docs at repository root

2. **Automate Where Possible**
   - Generate API docs from code
   - Auto-update diagrams from schema
   - Validate documentation in CI/CD

3. **Make Documentation Discoverable**
   - Clear navigation structure
   - Comprehensive table of contents
   - Cross-reference related docs

4. **Keep It Current**
   - Update docs with code changes
   - Version documentation
   - Archive outdated content

5. **Use Visual Aids**
   - Diagrams for complex flows
   - Screenshots for UI components
   - Code examples for APIs

## Maintenance Checklist

- [ ] All public APIs are documented
- [ ] README files are up to date
- [ ] Mermaid diagrams reflect current architecture
- [ ] OpenAPI specs match implementation
- [ ] Code examples are tested and working
- [ ] Links are valid and working
- [ ] Documentation is versioned
- [ ] Inline comments are comprehensive

## References

- [Documentation Standards](./references/documentation-standards.md)
- [API Documentation Patterns](./references/api-doc-patterns.md)
- [Diagram Conventions](./references/diagram-conventions.md)
- [README Template](./assets/templates/readme-template.md)
- [API Documentation Template](./assets/templates/api-doc-template.md)
- [Architecture Documentation Template](./assets/templates/architecture-doc-template.md)