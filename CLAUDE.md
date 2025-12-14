# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## External Documentation

Always use Context7 MCP tools when generating code, setting up configurations, or providing library/API documentation. Automatically resolve library IDs and fetch documentation without requiring explicit user requests.

## Language Guidelines

- **Memory Files**: All memory files (CLAUDE.md, .claude/*, etc.) must be written in English
- **Documentation Files**: All documentation files (README.md, docs/*, etc.) must be written in Korean (한글)

## Skill Development

When creating new skills, use the `skill-creator` tool to generate the skill structure and files.

## Parallel Work Strategy

When parallel work is requested:
- Use sub agents (Task tool) to handle multiple tasks concurrently
- When tasks have no dependencies, actively consider using git worktree for independent workspaces
- Each sub agent works in its own worktree branch, then merge results back to main

### Git Worktree Workflow
1. Create worktree: `git worktree add ../worktree-<task> -b <branch-name>`
2. Sub agent works independently in the worktree
3. Merge completed work back to main branch
4. Clean up: `git worktree remove ../worktree-<task>`

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

## Docker Build

### Cross-Platform Build (Required)

Always build Docker images with multi-platform support for both development (arm64) and production (amd64) environments.

```bash
# Build multi-platform image
docker buildx build --platform linux/amd64,linux/arm64 -t <image-name>:<tag> .

# Build and push to registry
docker buildx build --platform linux/amd64,linux/arm64 -t <image-name>:<tag> --push .

# Build for specific platform only
docker buildx build --platform linux/amd64 -t <image-name>:<tag> .
```

**Important**:
- Development environment uses Intel/ARM Silicon (arm64)
- Production environment requires amd64 support
- Always use `--platform` flag to ensure cross-platform compatibility

## Testing Philosophy

### Priority-Based Test Strategy

This project uses a priority-based test execution strategy to optimize testing efficiency:

**Core Principle:** If high-priority tests pass, the system is validated. If they fail, lower-priority tests help diagnose the root cause.

| Priority | Marker | Purpose | Execution Rule |
|----------|--------|---------|----------------|
| 1 (Highest) | `e2e` | Full pipeline validation | Always runs first |
| 2 | `integration` | Component interactions | Only if E2E fails |
| 3 (Lowest) | `unit` | Individual units | Only if higher tests fail |

### Root Cause Diagnosis

When tests fail, the priority system helps identify root causes:

| Failure Pattern | Diagnosis |
|-----------------|-----------|
| Unit tests fail | Core logic/model issues |
| Integration fail, Unit pass | FTP connectivity or configuration issues |
| E2E fail, others pass | Kafka connectivity, consumer logic, or pipeline integration issues |

## Testing

### Test Commands

```bash
# Activate virtual environment first
source .venv/bin/activate

# Recommended: Priority-based execution
./scripts/run_tests.sh           # E2E first, skip lower if pass
./scripts/run_tests.sh -v        # Verbose output
./scripts/run_tests.sh --coverage # With coverage report
./scripts/run_tests.sh --force-all # Run all tests regardless of results

# Using Makefile
make test-priority      # Priority-based execution
make test-priority-v    # Verbose priority execution
make test-priority-all  # Force all tests

# Individual test levels (for debugging)
make test-e2e           # E2E tests only
make test-integration   # Integration tests only
make test-unit          # Unit tests only

# Legacy commands (still supported)
pytest -m unit -v       # Unit tests only
pytest -m integration -v # Integration tests only
pytest -m e2e -v        # E2E tests only
pytest -v               # All tests (ordered by priority)

# With coverage report
pytest --cov=src/etl --cov-report=term-missing
```

### E2E Test Infrastructure

E2E tests require a Docker container running Kafka broker and ETL consumer.

```bash
# Start infrastructure
docker-compose -f docker-compose.test.yml up -d

# Check container status
docker ps | grep etl-test

# View logs
docker logs etl-test

# Stop infrastructure
docker-compose -f docker-compose.test.yml down
```

**Container Architecture:**
- Single container (`etl-test`) runs both Kafka and Consumer via supervisord
- Kafka broker: `localhost:9092`
- Consumer subscribes to: `test-transfer` topic

### Test Markers

| Marker | Description | Infrastructure Required |
|--------|-------------|------------------------|
| `unit` | Unit tests with mocks | None |
| `integration` | FTP integration tests | FTP Server |
| `e2e` | End-to-end tests | FTP + Kafka |

### FTP Test Server Configuration

**Server**: `192.168.1.4:21`

| User | Password | Permission Path |
|------|----------|-----------------|
| user01 | Votmdnjem01! | `/testserver01` |
| user02 | Votmdnjem02! | `/testserver02` |

**Important**: When writing FTP tests, always use the permission paths:
- Source (user01): `/testserver01/...`
- Destination (user02): `/testserver02/...`

### Test Environment File

Test configuration is stored in `.env.test`. This file contains:
- FTP server connection info
- Kafka settings
- DLQ configuration

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
