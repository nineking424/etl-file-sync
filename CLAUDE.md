# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## External Documentation

Always use Context7 MCP tools when generating code, setting up configurations, or providing library/API documentation. Automatically resolve library IDs and fetch documentation without requiring explicit user requests.

## Development Workflow

### Feature Development Process
1. **테스트 계획 수립**: 기능 요구사항에 맞는 테스트 케이스 정의
2. **기능 개발**: 테스트를 통과할 수 있는 코드 구현
3. **테스트 결과 확인**: 모든 테스트 통과 여부 검증

### Test Result in Commit
테스트가 동반되는 작업의 경우, 커밋 메시지에 테스트 결과를 포함:
```
feat: 사용자 인증 기능 추가

- JWT 토큰 기반 인증 구현
- 로그인/로그아웃 API 추가

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
- `feat`: 새로운 기능 추가
- `fix`: 버그 수정
- `docs`: 문서 변경
- `refactor`: 코드 리팩토링 (기능 변경 없음)
- `test`: 테스트 추가/수정
- `chore`: 빌드, 설정 등 기타 변경

**Example:**
```
feat: 사용자 인증 기능 추가

- JWT 토큰 기반 인증 구현
- 로그인/로그아웃 API 추가
```
