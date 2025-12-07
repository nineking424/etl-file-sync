# GitHub Actions 패턴

GitHub Actions를 효과적으로 활용하기 위한 패턴과 모범 사례를 설명합니다.

## 기본 구조

### 워크플로우 파일 위치
```
.github/
└── workflows/
    ├── ci.yml
    ├── cd.yml
    ├── release.yml
    └── security-scan.yml
```

## 트리거 패턴

### 1. Push 및 Pull Request

```yaml
on:
  push:
    branches:
      - main
      - develop
      - 'release/**'
    paths:
      - 'src/**'
      - 'package.json'
      - '.github/workflows/**'
    paths-ignore:
      - '**.md'
      - 'docs/**'

  pull_request:
    branches:
      - main
    types:
      - opened
      - synchronize
      - reopened
```

### 2. 스케줄 실행

```yaml
on:
  schedule:
    # 매일 오전 2시 (UTC)
    - cron: '0 2 * * *'
    # 매주 월요일 오전 9시 (UTC)
    - cron: '0 9 * * 1'

  workflow_dispatch:  # 수동 실행 허용
    inputs:
      environment:
        description: 'Environment to deploy'
        required: true
        type: choice
        options:
          - development
          - staging
          - production
```

### 3. Release 트리거

```yaml
on:
  release:
    types:
      - published
      - created
```

## Job 패턴

### 1. Matrix 전략

```yaml
jobs:
  test:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, windows-latest, macos-latest]
        node-version: [16.x, 18.x, 20.x]
        include:
          - os: ubuntu-latest
            node-version: 18.x
            coverage: true
        exclude:
          - os: windows-latest
            node-version: 16.x

    steps:
      - uses: actions/checkout@v4
      - name: Setup Node.js ${{ matrix.node-version }}
        uses: actions/setup-node@v4
        with:
          node-version: ${{ matrix.node-version }}
      - run: npm ci
      - run: npm test

      - name: Upload coverage
        if: matrix.coverage
        uses: codecov/codecov-action@v3
```

### 2. Job 의존성

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo "Building..."

  test:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - run: echo "Testing..."

  deploy-staging:
    needs: [build, test]
    runs-on: ubuntu-latest
    steps:
      - run: echo "Deploying to staging..."

  deploy-production:
    needs: deploy-staging
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - run: echo "Deploying to production..."
```

### 3. 조건부 실행

```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    if: |
      github.event_name == 'push' &&
      github.ref == 'refs/heads/main' &&
      !contains(github.event.head_commit.message, '[skip ci]')

    steps:
      - name: Deploy only on main branch
        if: success()
        run: echo "Deploying..."

      - name: Cleanup on failure
        if: failure()
        run: echo "Cleaning up..."
```

## 재사용 가능한 워크플로우

### Reusable Workflow 정의

```yaml
# .github/workflows/reusable-build.yml
name: Reusable Build

on:
  workflow_call:
    inputs:
      node-version:
        required: true
        type: string
      environment:
        required: false
        type: string
        default: 'development'
    outputs:
      artifact-name:
        description: "Built artifact name"
        value: ${{ jobs.build.outputs.artifact }}
    secrets:
      npm-token:
        required: true

jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      artifact: ${{ steps.build.outputs.artifact }}

    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: ${{ inputs.node-version }}

      - name: Install dependencies
        run: npm ci
        env:
          NPM_TOKEN: ${{ secrets.npm-token }}

      - name: Build
        id: build
        run: |
          npm run build
          echo "artifact=build-${{ github.sha }}" >> $GITHUB_OUTPUT
```

### Reusable Workflow 사용

```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  build-node-16:
    uses: ./.github/workflows/reusable-build.yml
    with:
      node-version: '16.x'
      environment: 'development'
    secrets:
      npm-token: ${{ secrets.NPM_TOKEN }}

  build-node-18:
    uses: ./.github/workflows/reusable-build.yml
    with:
      node-version: '18.x'
      environment: 'production'
    secrets:
      npm-token: ${{ secrets.NPM_TOKEN }}
```

## 캐싱 패턴

### 1. npm 캐싱

```yaml
- uses: actions/setup-node@v4
  with:
    node-version: '18.x'
    cache: 'npm'  # 자동 캐싱

- name: Cache node modules
  uses: actions/cache@v3
  with:
    path: ~/.npm
    key: ${{ runner.os }}-node-${{ hashFiles('**/package-lock.json') }}
    restore-keys: |
      ${{ runner.os }}-node-
```

### 2. Maven 캐싱

```yaml
- uses: actions/setup-java@v4
  with:
    java-version: '17'
    cache: 'maven'
```

### 3. Docker Layer 캐싱

```yaml
- name: Set up Docker Buildx
  uses: docker/setup-buildx-action@v3

- name: Build and push
  uses: docker/build-push-action@v5
  with:
    context: .
    push: true
    tags: user/app:latest
    cache-from: type=gha
    cache-to: type=gha,mode=max
```

## 환경 및 시크릿

### 1. Environment 사용

```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment:
      name: production
      url: https://example.com

    steps:
      - name: Deploy
        run: echo "Deploying to ${{ vars.ENVIRONMENT_NAME }}"
        env:
          API_KEY: ${{ secrets.PRODUCTION_API_KEY }}
```

### 2. 동적 환경

```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment:
      name: ${{ github.event.inputs.environment }}

    steps:
      - name: Deploy to ${{ github.event.inputs.environment }}
        run: ./deploy.sh
```

## Docker 빌드 패턴

### 멀티 플랫폼 빌드

```yaml
- name: Set up QEMU
  uses: docker/setup-qemu-action@v3

- name: Set up Docker Buildx
  uses: docker/setup-buildx-action@v3

- name: Build multi-platform images
  uses: docker/build-push-action@v5
  with:
    context: .
    platforms: linux/amd64,linux/arm64
    push: true
    tags: |
      user/app:latest
      user/app:${{ github.sha }}
```

## 아티팩트 관리

### 1. 아티팩트 업로드

```yaml
- name: Upload artifact
  uses: actions/upload-artifact@v3
  with:
    name: build-files
    path: |
      dist/
      build/
    retention-days: 30
```

### 2. 아티팩트 다운로드

```yaml
- name: Download artifact
  uses: actions/download-artifact@v3
  with:
    name: build-files
    path: ./downloaded
```

## 보안 스캔 통합

### 1. Dependency Scanning

```yaml
- name: Run Snyk
  uses: snyk/actions/node@master
  env:
    SNYK_TOKEN: ${{ secrets.SNYK_TOKEN }}
  with:
    args: --severity-threshold=high
```

### 2. Container Scanning

```yaml
- name: Run Trivy scanner
  uses: aquasecurity/trivy-action@master
  with:
    image-ref: 'user/app:${{ github.sha }}'
    format: 'sarif'
    output: 'trivy-results.sarif'

- name: Upload to Security tab
  uses: github/codeql-action/upload-sarif@v2
  with:
    sarif_file: 'trivy-results.sarif'
```

## 알림 패턴

### Slack 알림

```yaml
- name: Slack Notification
  uses: slackapi/slack-github-action@v1
  with:
    payload: |
      {
        "text": "Build ${{ job.status }}: ${{ github.repository }}",
        "blocks": [
          {
            "type": "section",
            "text": {
              "type": "mrkdwn",
              "text": "*Build Status:* ${{ job.status }}\n*Repository:* ${{ github.repository }}\n*Branch:* ${{ github.ref_name }}"
            }
          }
        ]
      }
  env:
    SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
```

## 성능 최적화

### 1. 병렬 실행

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        shard: [1, 2, 3, 4]
    steps:
      - run: npm test -- --shard=${{ matrix.shard }}/4
```

### 2. 조건부 실행으로 시간 절약

```yaml
- name: Skip if no code changes
  uses: dorny/paths-filter@v2
  id: changes
  with:
    filters: |
      src:
        - 'src/**'

- name: Run tests
  if: steps.changes.outputs.src == 'true'
  run: npm test
```

## 체크리스트

- [ ] 적절한 트리거 설정
- [ ] Job 의존성 명확히 정의
- [ ] 캐싱 전략 적용
- [ ] 시크릿 안전하게 관리
- [ ] 아티팩트 적절히 활용
- [ ] 보안 스캔 통합
- [ ] 알림 설정
- [ ] 워크플로우 재사용성 고려
- [ ] 실행 시간 최적화
- [ ] 로그 가독성 확보
