# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## External Documentation

Always use Context7 MCP tools when generating code, setting up configurations, or providing library/API documentation. Automatically resolve library IDs and fetch documentation without requiring explicit user requests.

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
