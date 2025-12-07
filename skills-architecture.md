# SW Development Skills Architecture

## 전체 디렉토리 구조

```
skills/
├── workflow/                    # 핵심 개발 워크플로우
│   ├── prompt-enhancer/
│   ├── developer/
│   ├── tester/
│   ├── code-reviewer/
│   ├── refactorer/
│   └── document-maintainer/
│
├── quality-gate/                # 품질 관문
│   ├── security-auditor/
│   ├── performance-analyzer/
│   └── debugger/
│
├── design/                      # 설계 도구
│   ├── api-designer/
│   ├── architecture-advisor/
│   └── cicd-designer/
│
└── domain/                      # 기술 특화
    ├── kafka-expert/
    ├── k8s-specialist/
    ├── database-optimizer/
    ├── airflow-architect/
    └── etl-pipeline-builder/
```

---

## Phase 1: 핵심 워크플로우 Skills

### 1. prompt-enhancer

```
prompt-enhancer/
├── SKILL.md
├── references/
│   ├── enhancement-patterns.md      # 프롬프트 개선 패턴
│   ├── context-templates.md         # 컨텍스트 템플릿
│   └── anti-patterns.md             # 피해야 할 패턴
└── assets/
    └── prompt-templates/            # 도메인별 프롬프트 템플릿
```

**SKILL.md 구조:**
```yaml
---
name: prompt-enhancer
description: |
  Enhance and optimize prompts for better AI-assisted development outcomes.
  Use when: (1) User provides vague or incomplete requirements, (2) Task needs
  clarification before development, (3) Converting user intent into actionable
  specifications, (4) Structuring complex multi-step requests.
---
```

**핵심 기능:**
- 모호한 요구사항 → 명확한 스펙으로 변환
- 누락된 컨텍스트 식별 및 질문 생성
- 작업 분해 (큰 작업 → 단계별 태스크)
- 제약사항/엣지케이스 도출


### 2. developer

```
developer/
├── SKILL.md
├── scripts/
│   ├── scaffold.py                  # 프로젝트 스캐폴딩
│   ├── generate_boilerplate.py      # 보일러플레이트 생성
│   └── dependency_resolver.py       # 의존성 분석
├── references/
│   ├── coding-standards.md          # 코딩 표준
│   ├── design-patterns.md           # 디자인 패턴 가이드
│   ├── error-handling.md            # 에러 처리 패턴
│   └── logging-conventions.md       # 로깅 컨벤션
└── assets/
    └── templates/                   # 코드 템플릿
        ├── java/
        ├── python/
        └── kotlin/
```

**SKILL.md 구조:**
```yaml
---
name: developer
description: |
  Generate production-quality backend code following best practices.
  Use when: (1) Writing new features or modules, (2) Implementing business logic,
  (3) Creating data models or DTOs, (4) Building service layers,
  (5) Integrating external APIs or libraries.
---
```

**핵심 기능:**
- 언어별 코딩 컨벤션 적용
- 디자인 패턴 기반 코드 생성
- 에러 핸들링/로깅 자동 포함
- 의존성 주입 패턴 적용


### 3. tester

```
tester/
├── SKILL.md
├── scripts/
│   ├── test_generator.py            # 테스트 코드 생성
│   ├── coverage_analyzer.py         # 커버리지 분석
│   └── mutation_tester.py           # 뮤테이션 테스트
├── references/
│   ├── testing-strategies.md        # 테스트 전략 (Unit/Integration/E2E)
│   ├── mocking-patterns.md          # 모킹 패턴
│   ├── test-data-patterns.md        # 테스트 데이터 생성 패턴
│   └── assertion-patterns.md        # Assertion 패턴
└── assets/
    └── fixtures/                    # 테스트 픽스처 템플릿
```

**SKILL.md 구조:**
```yaml
---
name: tester
description: |
  Generate comprehensive test suites with high coverage and meaningful assertions.
  Use when: (1) Writing unit tests for new code, (2) Creating integration tests,
  (3) Generating test data/fixtures, (4) Analyzing test coverage gaps,
  (5) Setting up test infrastructure (mocks, stubs, fakes).
---
```

**핵심 기능:**
- 경계값/엣지케이스 자동 도출
- Given-When-Then 패턴 적용
- Mock/Stub 자동 생성
- 커버리지 분석 및 누락 테스트 제안


### 4. code-reviewer

```
code-reviewer/
├── SKILL.md
├── scripts/
│   ├── static_analyzer.py           # 정적 분석
│   ├── complexity_checker.py        # 복잡도 분석
│   └── smell_detector.py            # 코드 스멜 탐지
├── references/
│   ├── review-checklist.md          # 리뷰 체크리스트
│   ├── code-smells.md               # 코드 스멜 카탈로그
│   ├── clean-code-principles.md     # 클린코드 원칙
│   └── review-comment-templates.md  # 리뷰 코멘트 템플릿
└── assets/
    └── report-templates/            # 리뷰 리포트 템플릿
```

**SKILL.md 구조:**
```yaml
---
name: code-reviewer
description: |
  Perform thorough code reviews with actionable feedback and improvement suggestions.
  Use when: (1) Reviewing pull requests or code changes, (2) Checking code quality,
  (3) Identifying bugs or potential issues, (4) Enforcing coding standards,
  (5) Suggesting improvements or refactoring opportunities.
---
```

**핵심 기능:**
- 버그/취약점 탐지
- 복잡도 분석 (Cyclomatic, Cognitive)
- 네이밍/구조 개선 제안
- 리뷰 우선순위 분류 (Critical/Major/Minor)


### 5. refactorer

```
refactorer/
├── SKILL.md
├── scripts/
│   ├── extract_method.py            # 메서드 추출
│   ├── inline_variable.py           # 변수 인라인
│   └── dependency_analyzer.py       # 의존성 분석
├── references/
│   ├── refactoring-catalog.md       # 리팩토링 카탈로그
│   ├── safe-refactoring.md          # 안전한 리팩토링 절차
│   └── migration-patterns.md        # 마이그레이션 패턴
└── assets/
    └── before-after-examples/       # 리팩토링 예시
```

**SKILL.md 구조:**
```yaml
---
name: refactorer
description: |
  Improve code structure and quality through systematic refactoring.
  Use when: (1) Reducing code complexity, (2) Eliminating duplication,
  (3) Improving readability, (4) Applying design patterns,
  (5) Breaking down large classes/methods, (6) Modernizing legacy code.
---
```

**핵심 기능:**
- Martin Fowler 리팩토링 카탈로그 적용
- 점진적 리팩토링 계획 수립
- 테스트 보존 전략
- 영향 범위 분석


### 6. document-maintainer

```
document-maintainer/
├── SKILL.md
├── scripts/
│   ├── doc_generator.py             # 문서 자동 생성
│   ├── sync_checker.py              # 코드-문서 동기화 체크
│   └── changelog_generator.py       # 변경로그 생성
├── references/
│   ├── documentation-standards.md   # 문서화 표준
│   ├── api-doc-patterns.md          # API 문서 패턴
│   └── diagram-conventions.md       # 다이어그램 컨벤션
└── assets/
    └── templates/
        ├── readme-template.md
        ├── api-doc-template.md
        └── architecture-doc-template.md
```

**SKILL.md 구조:**
```yaml
---
name: document-maintainer
description: |
  Maintain and synchronize technical documentation with codebase.
  Use when: (1) Generating API documentation, (2) Updating README files,
  (3) Creating architecture diagrams (Mermaid), (4) Writing changelog entries,
  (5) Documenting code changes, (6) Checking doc-code consistency.
---
```

**핵심 기능:**
- 코드 변경 시 문서 자동 업데이트
- Javadoc/Docstring 생성
- Mermaid 다이어그램 생성/업데이트
- API 스펙 문서화 (OpenAPI)

---

## Phase 2: 품질 관문 Skills

### 7. security-auditor

```
security-auditor/
├── SKILL.md
├── scripts/
│   ├── secret_scanner.py            # 시크릿 스캔
│   ├── dependency_checker.py        # 취약 의존성 체크
│   └── sast_analyzer.py             # 정적 보안 분석
├── references/
│   ├── owasp-top10.md               # OWASP Top 10
│   ├── secure-coding.md             # 시큐어 코딩 가이드
│   ├── vulnerability-patterns.md    # 취약점 패턴
│   └── remediation-guide.md         # 취약점 수정 가이드
└── assets/
    └── security-report-template.md
```

**SKILL.md 구조:**
```yaml
---
name: security-auditor
description: |
  Identify security vulnerabilities and provide remediation guidance.
  Use when: (1) Scanning for security issues, (2) Checking OWASP compliance,
  (3) Reviewing authentication/authorization code, (4) Detecting secrets exposure,
  (5) Auditing dependency vulnerabilities, (6) Validating input sanitization.
---
```

**핵심 기능:**
- OWASP Top 10 체크
- SQL Injection, XSS, CSRF 탐지
- 하드코딩된 시크릿 탐지
- 취약한 의존성 식별


### 8. performance-analyzer

```
performance-analyzer/
├── SKILL.md
├── scripts/
│   ├── complexity_analyzer.py       # 시간/공간 복잡도 분석
│   ├── bottleneck_finder.py         # 병목 탐지
│   └── query_analyzer.py            # 쿼리 성능 분석
├── references/
│   ├── performance-patterns.md      # 성능 패턴
│   ├── anti-patterns.md             # 성능 안티패턴
│   ├── caching-strategies.md        # 캐싱 전략
│   └── optimization-techniques.md   # 최적화 기법
└── assets/
    └── benchmark-templates/
```

**SKILL.md 구조:**
```yaml
---
name: performance-analyzer
description: |
  Analyze and optimize code performance with actionable recommendations.
  Use when: (1) Identifying performance bottlenecks, (2) Analyzing time/space complexity,
  (3) Optimizing database queries, (4) Reviewing memory usage patterns,
  (5) Suggesting caching strategies, (6) Profiling hot paths.
---
```

**핵심 기능:**
- Big-O 복잡도 분석
- N+1 쿼리 탐지
- 메모리 릭 패턴 식별
- 캐싱/배치 최적화 제안


### 9. debugger

```
debugger/
├── SKILL.md
├── scripts/
│   ├── log_parser.py                # 로그 파싱
│   ├── stack_trace_analyzer.py      # 스택트레이스 분석
│   └── root_cause_finder.py         # 근본 원인 분석
├── references/
│   ├── debugging-strategies.md      # 디버깅 전략
│   ├── common-errors.md             # 자주 발생하는 에러
│   └── troubleshooting-guide.md     # 트러블슈팅 가이드
└── assets/
    └── debug-checklist.md
```

**SKILL.md 구조:**
```yaml
---
name: debugger
description: |
  Analyze errors and find root causes with systematic debugging approach.
  Use when: (1) Analyzing stack traces, (2) Interpreting error messages,
  (3) Finding root cause of bugs, (4) Parsing and analyzing logs,
  (5) Reproducing intermittent issues, (6) Debugging race conditions.
---
```

**핵심 기능:**
- 스택트레이스 해석
- 에러 메시지 분석
- 5 Whys 근본 원인 분석
- 재현 시나리오 도출

---

## Phase 3: 설계 도구 Skills

### 10. api-designer

```
api-designer/
├── SKILL.md
├── scripts/
│   ├── openapi_generator.py         # OpenAPI 스펙 생성
│   ├── grpc_generator.py            # gRPC proto 생성
│   └── api_validator.py             # API 설계 검증
├── references/
│   ├── rest-best-practices.md       # REST API 모범사례
│   ├── api-versioning.md            # API 버저닝 전략
│   ├── error-response-patterns.md   # 에러 응답 패턴
│   └── pagination-patterns.md       # 페이지네이션 패턴
└── assets/
    └── templates/
        ├── openapi-template.yaml
        └── proto-template.proto
```

**SKILL.md 구조:**
```yaml
---
name: api-designer
description: |
  Design RESTful APIs and gRPC services following industry best practices.
  Use when: (1) Designing new API endpoints, (2) Creating OpenAPI/Swagger specs,
  (3) Defining gRPC services, (4) Planning API versioning strategy,
  (5) Designing error response structures, (6) Implementing pagination.
---
```

**핵심 기능:**
- RESTful 설계 원칙 적용
- OpenAPI 3.0 스펙 생성
- gRPC proto 파일 생성
- API 버저닝 전략 수립


### 11. architecture-advisor

```
architecture-advisor/
├── SKILL.md
├── scripts/
│   ├── dependency_graph.py          # 의존성 그래프 생성
│   ├── module_analyzer.py           # 모듈 분석
│   └── coupling_checker.py          # 결합도 체크
├── references/
│   ├── architecture-patterns.md     # 아키텍처 패턴 (Hexagonal, Clean, etc.)
│   ├── ddd-patterns.md              # DDD 패턴
│   ├── microservices-patterns.md    # MSA 패턴
│   └── decision-records.md          # ADR 작성 가이드
└── assets/
    └── diagram-templates/
        ├── c4-template.md
        └── sequence-template.md
```

**SKILL.md 구조:**
```yaml
---
name: architecture-advisor
description: |
  Provide architecture guidance and design pattern recommendations.
  Use when: (1) Designing system architecture, (2) Choosing design patterns,
  (3) Evaluating architectural trade-offs, (4) Creating C4 diagrams,
  (5) Writing Architecture Decision Records (ADR), (6) Analyzing coupling/cohesion.
---
```

**핵심 기능:**
- 아키텍처 패턴 추천
- C4 다이어그램 생성
- ADR 작성
- 결합도/응집도 분석


### 12. cicd-designer

```
cicd-designer/
├── SKILL.md
├── scripts/
│   ├── pipeline_generator.py        # 파이프라인 생성
│   ├── workflow_validator.py        # 워크플로우 검증
│   └── secret_manager.py            # 시크릿 관리
├── references/
│   ├── github-actions-patterns.md   # GitHub Actions 패턴
│   ├── jenkins-patterns.md          # Jenkins 패턴
│   ├── argocd-patterns.md           # ArgoCD 패턴
│   └── deployment-strategies.md     # 배포 전략 (Blue-Green, Canary)
└── assets/
    └── templates/
        ├── github-actions/
        ├── jenkins/
        └── argocd/
```

**SKILL.md 구조:**
```yaml
---
name: cicd-designer
description: |
  Design and implement CI/CD pipelines for automated build, test, and deployment.
  Use when: (1) Creating GitHub Actions workflows, (2) Setting up Jenkins pipelines,
  (3) Configuring ArgoCD applications, (4) Implementing deployment strategies,
  (5) Managing pipeline secrets, (6) Optimizing build performance.
---
```

**핵심 기능:**
- GitHub Actions 워크플로우 생성
- Jenkins Pipeline as Code
- ArgoCD Application 설정
- Blue-Green/Canary 배포 설정

---

## Phase 4: 기술 특화 Skills

### 13. kafka-expert

```
kafka-expert/
├── SKILL.md
├── scripts/
│   ├── topic_designer.py            # 토픽 설계
│   ├── consumer_analyzer.py         # 컨슈머 분석
│   └── config_validator.py          # 설정 검증
├── references/
│   ├── topic-design-patterns.md     # 토픽 설계 패턴
│   ├── partition-strategies.md      # 파티션 전략
│   ├── consumer-patterns.md         # 컨슈머 패턴
│   ├── k8s-kafka-setup.md           # K8s 환경 Kafka 설정
│   └── troubleshooting.md           # 트러블슈팅 가이드
└── assets/
    └── config-templates/
        ├── producer-config.properties
        ├── consumer-config.properties
        └── kraft-config.properties
```

**SKILL.md 구조:**
```yaml
---
name: kafka-expert
description: |
  Design and optimize Apache Kafka configurations and architectures.
  Use when: (1) Designing Kafka topics and partitions, (2) Configuring producers/consumers,
  (3) Setting up Kafka in Kubernetes, (4) Troubleshooting Kafka issues,
  (5) Optimizing throughput/latency, (6) Implementing exactly-once semantics.
---
```

**핵심 기능:**
- 토픽/파티션 설계
- 컨슈머 그룹 최적화
- K8s LoadBalancer 외부 접근 설정
- KRaft 모드 설정


### 14. k8s-specialist

```
k8s-specialist/
├── SKILL.md
├── scripts/
│   ├── manifest_generator.py        # 매니페스트 생성
│   ├── helm_chart_creator.py        # Helm 차트 생성
│   └── resource_calculator.py       # 리소스 계산
├── references/
│   ├── resource-patterns.md         # 리소스 패턴
│   ├── networking-patterns.md       # 네트워킹 패턴
│   ├── security-patterns.md         # 보안 패턴 (RBAC, NetworkPolicy)
│   ├── scaling-patterns.md          # 스케일링 패턴 (HPA, VPA)
│   └── troubleshooting.md           # 트러블슈팅 가이드
└── assets/
    └── templates/
        ├── deployment/
        ├── service/
        ├── configmap/
        └── helm/
```

**SKILL.md 구조:**
```yaml
---
name: k8s-specialist
description: |
  Design and manage Kubernetes resources and Helm charts.
  Use when: (1) Creating K8s manifests, (2) Building Helm charts,
  (3) Configuring networking (Service, Ingress), (4) Setting up RBAC,
  (5) Implementing HPA/VPA, (6) Troubleshooting pod issues.
---
```

**핵심 기능:**
- Deployment/Service/ConfigMap 생성
- Helm 차트 구조화
- 리소스 Request/Limit 최적화
- NetworkPolicy/RBAC 설정


### 15. database-optimizer

```
database-optimizer/
├── SKILL.md
├── scripts/
│   ├── query_analyzer.py            # 쿼리 분석
│   ├── index_advisor.py             # 인덱스 추천
│   └── execution_plan_parser.py     # 실행계획 파서
├── references/
│   ├── oracle-optimization.md       # Oracle 최적화
│   ├── duckdb-patterns.md           # DuckDB 패턴
│   ├── indexing-strategies.md       # 인덱싱 전략
│   ├── jdbc-batch-patterns.md       # JDBC 배치 패턴
│   └── connection-pool-tuning.md    # 커넥션 풀 튜닝
└── assets/
    └── query-templates/
```

**SKILL.md 구조:**
```yaml
---
name: database-optimizer
description: |
  Optimize database queries and configurations for Oracle and DuckDB.
  Use when: (1) Analyzing slow queries, (2) Designing indexes,
  (3) Interpreting execution plans, (4) Tuning JDBC batch operations,
  (5) Configuring connection pools, (6) Optimizing bulk operations.
---
```

**핵심 기능:**
- 실행계획 분석
- 인덱스 설계/추천
- JDBC 배치 vs MyBatis ExecutorType.BATCH 비교
- 커넥션 풀 튜닝


### 16. airflow-architect

```
airflow-architect/
├── SKILL.md
├── scripts/
│   ├── dag_generator.py             # DAG 생성
│   ├── task_dependency_analyzer.py  # 태스크 의존성 분석
│   └── schedule_optimizer.py        # 스케줄 최적화
├── references/
│   ├── dag-patterns.md              # DAG 설계 패턴
│   ├── operator-guide.md            # Operator 가이드
│   ├── best-practices.md            # 모범 사례
│   └── troubleshooting.md           # 트러블슈팅
└── assets/
    └── dag-templates/
```

**SKILL.md 구조:**
```yaml
---
name: airflow-architect
description: |
  Design and optimize Apache Airflow DAGs and workflows.
  Use when: (1) Creating new DAGs, (2) Designing task dependencies,
  (3) Implementing sensors and operators, (4) Optimizing parallel execution,
  (5) Managing XComs and variables, (6) Troubleshooting failed tasks.
---
```

**핵심 기능:**
- DAG 구조 설계
- 태스크 의존성 최적화
- 병렬 실행 전략
- 동적 DAG 생성


### 17. etl-pipeline-builder

```
etl-pipeline-builder/
├── SKILL.md
├── scripts/
│   ├── pipeline_designer.py         # 파이프라인 설계
│   ├── schema_mapper.py             # 스키마 매핑
│   └── data_quality_checker.py      # 데이터 품질 체크
├── references/
│   ├── etl-patterns.md              # ETL 패턴
│   ├── elt-patterns.md              # ELT 패턴
│   ├── cdc-patterns.md              # CDC 패턴
│   ├── error-handling.md            # 에러 처리 전략
│   └── idempotency-patterns.md      # 멱등성 패턴
└── assets/
    └── pipeline-templates/
```

**SKILL.md 구조:**
```yaml
---
name: etl-pipeline-builder
description: |
  Design robust ETL/ELT pipelines for data integration.
  Use when: (1) Building data pipelines, (2) Designing source-to-sink flows,
  (3) Implementing CDC (Change Data Capture), (4) Handling data quality issues,
  (5) Ensuring idempotent processing, (6) Managing schema evolution.
---
```

**핵심 기능:**
- ETL/ELT 파이프라인 설계
- CDC 패턴 구현
- 멱등성 보장 설계
- 스키마 진화 처리

---

## 공통 References (Cross-Cutting)

모든 Skills에서 참조할 수 있는 공통 레퍼런스:

```
common/
├── references/
│   ├── git-conventions.md           # Git 커밋/브랜치 컨벤션
│   ├── logging-standards.md         # 로깅 표준
│   ├── error-codes.md               # 에러 코드 체계
│   └── naming-conventions.md        # 네이밍 컨벤션
└── assets/
    └── language-templates/
        ├── java/
        ├── python/
        └── kotlin/
```

---

## 개발 우선순위

### 1차 (핵심 워크플로우) - 2주
1. prompt-enhancer
2. developer
3. tester
4. code-reviewer
5. document-maintainer

### 2차 (품질 강화) - 1주
6. security-auditor
7. performance-analyzer
8. refactorer

### 3차 (설계 도구) - 1주
9. api-designer
10. cicd-designer
11. architecture-advisor

### 4차 (기술 특화) - 필요시
12. kafka-expert
13. k8s-specialist
14. database-optimizer
15. airflow-architect
16. etl-pipeline-builder

---

## 워크플로우 연계

```
User Request
    │
    ▼
┌─────────────────┐
│ prompt-enhancer │ ─── 요구사항 명확화
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────────┐
│    developer    │ ◄───│ architecture-advisor│
└────────┬────────┘     │ api-designer        │
         │              │ domain skills       │
         ▼              └─────────────────────┘
┌─────────────────┐
│     tester      │ ─── 테스트 생성
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  code-reviewer  │ ─── 코드 리뷰
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌────────┐  ┌───────────────────┐
│ Pass   │  │ Fail → refactorer │
└───┬────┘  └───────────────────┘
    │
    ▼
┌───────────────────────────────┐
│ Quality Gate (parallel)       │
│ ├─ security-auditor           │
│ ├─ performance-analyzer       │
│ └─ debugger (if needed)       │
└────────────────┬──────────────┘
                 │
                 ▼
┌─────────────────────┐
│ document-maintainer │ ─── 문서 동기화
└─────────────────────┘
```