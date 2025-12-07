# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## External Documentation

Always use Context7 MCP tools when generating code, setting up configurations, or providing library/API documentation. Automatically resolve library IDs and fetch documentation without requiring explicit user requests.

## Language Guidelines

- **Memory Files**: All memory files (CLAUDE.md, .claude/*, etc.) must be written in English
- **Documentation Files**: All documentation files (README.md, docs/*, etc.) must be written in Korean (한글)

## Skill Development

When creating new skills, use the `skill-creator` tool to generate the skill structure and files.

## Development Workflow

### Feature Development Process
1. **Test Planning**: Define test cases based on feature requirements
2. **Feature Development**: Implement code to pass the tests
3. **Test Verification**: Verify all tests pass

### Test Result in Commit
When work involves tests, include test results in the commit message:
```
feat: Add user authentication

- Implement JWT token-based authentication
- Add login/logout API

Test: 5 passed, 0 failed
```

## Git Workflow

All changes must be committed to git and pushed to the remote repository.

### Rules
- **Mandatory Commit**: Every code change must be committed to git
- **Mandatory Push**: Always push commits to the remote repository after committing
- **Commit Messages**: Write clear, meaningful commit messages in Korean (한글)
- **Atomic Commits**: Make commits in logical units (one feature/fix per commit)

### Commit Message Format
```
<type>: <subject>

<body>
```

**Type Prefixes:**
- `feat`: Add new feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring (no functional changes)
- `test`: Add/modify tests
- `chore`: Build, config, and other changes

**Example:**
```
feat: Add user authentication

- Implement JWT token-based authentication
- Add login/logout API
```
