---
name: prompt-enhancer
description: |
  Enhance backend development prompts by analyzing project context (architecture, dependencies, infrastructure, existing patterns).
  Use when: (1) User provides brief requests like "API 만들어줘", "Kafka 연동해줘", "배치 작업 추가해줘",
  (2) Request lacks implementation details, (3) Task requires understanding existing codebase architecture,
  (4) Complex infrastructure integration is needed (K8s, Kafka, Airflow, DB).
---

# Prompt Enhancer (Backend)

Transform brief backend development requests into detailed, architecture-aware requirements. Present enhanced requirements to user for confirmation before implementation.

## Core Workflow

### Step 1: Analyze Project Context

```bash
view /mnt/user-data/uploads
```

**Gather key information:**
- Build system: pom.xml, build.gradle, requirements.txt, pyproject.toml
- Architecture: Package structure, layer separation
- Infrastructure: docker-compose.yml, k8s manifests, Helm charts
- Database: Schema files, migration scripts, entity classes
- Messaging: Kafka configs, topic definitions
- Existing patterns: Error handling, logging, transaction management

### Step 2: Extract Request Intent

Identify:
- **Type**: API, Batch, Event processor, Integration, Migration
- **Scope**: Single endpoint, full domain, cross-service
- **Infrastructure**: DB, Kafka, Cache, External API
- **NFR**: Performance, reliability, idempotency requirements

### Step 3: Build Enhanced Requirements

```markdown
# [기능명] 구현 요구사항

## 📋 프로젝트 컨텍스트
- Stack: [Java/Kotlin/Python version]
- Framework: [Spring Boot/FastAPI/etc]
- Architecture: [Hexagonal/Layered/DDD]
- Messaging: [Kafka/RabbitMQ]
- Database: [Oracle/PostgreSQL/DuckDB]
- Infra: [K8s/Docker]

## 🎯 구현 범위

### 주요 기능
1. [Main feature]
2. [Main feature]

### 모듈 구조
```
[Expected module/package structure]
```

## 📝 상세 요구사항

### 1. [Layer/Component]
- **위치**: [path]
- **목적**: [purpose]
- **구현 내용**: [details]
- **기존 패턴**: [reference]

## ⚙️ 인프라 요구사항
- [Kafka topic/DB table/K8s resource 등]

## ✅ 성공 기준
- [ ] [Acceptance criteria]
- [ ] 기존 아키텍처 일관성 유지
- [ ] 단위 테스트 + 통합 테스트

## 🔍 확인 사항
- [Clarifications needed]

---
이 요구사항으로 진행할까요?
```

### Step 4: Present and Wait for Confirmation

**Do NOT implement** until user confirms.

---

## Analysis Patterns by Stack

### Spring Boot (Java/Kotlin)

**Detect**: pom.xml with spring-boot, build.gradle with spring

**Key context:**
- Architecture (Hexagonal, Layered, DDD)
- Data access (JPA, MyBatis, JDBC)
- Transaction management pattern
- Exception handling strategy
- API documentation (SpringDoc/Swagger)

**Enhanced template:**
```markdown
## 구현 범위

### API Layer
- Controller: [package]/controller/[Name]Controller.java
- Request/Response DTO: [package]/dto/
- Validation: Jakarta Bean Validation

### Application Layer
- Service: [package]/service/[Name]Service.java
- UseCase: [if hexagonal]
- Transaction boundary: @Transactional 범위

### Domain Layer
- Entity: [package]/domain/[Name].java
- Repository Interface: [package]/repository/
- Domain Service: [if complex logic]

### Infrastructure Layer
- Repository Impl: [JPA/MyBatis]
- External API Client: [if needed]
- Kafka Producer/Consumer: [if messaging]

### Configuration
- Properties: application.yml
- Bean Configuration: @Configuration classes

## 성공 기준
✅ REST API 설계 (RESTful conventions)
✅ 트랜잭션 경계 명확화
✅ 예외 처리 (@ControllerAdvice)
✅ 로깅 (MDC context)
✅ OpenAPI 문서 자동 생성
✅ 단위 테스트 (Mockito) + 통합 테스트 (@SpringBootTest)
```

### FastAPI (Python)

**Detect**: requirements.txt with fastapi, pyproject.toml

**Key context:**
- Async/Sync pattern
- ORM (SQLAlchemy, Tortoise)
- Dependency injection pattern
- Pydantic model conventions

**Enhanced template:**
```markdown
## 구현 범위

### API Layer
- Router: app/api/v1/[name].py
- Schema: app/schemas/[name].py (Pydantic)
- Dependencies: app/api/deps.py

### Service Layer
- Service: app/services/[name]_service.py
- Business logic with type hints

### Data Layer
- Model: app/models/[name].py (SQLAlchemy)
- Repository: app/repositories/[name]_repo.py
- CRUD operations

### Core
- Config: app/core/config.py
- Exceptions: app/core/exceptions.py

## 성공 기준
✅ Pydantic v2 schema validation
✅ Async database operations
✅ Proper HTTP status codes
✅ OpenAPI documentation
✅ pytest + pytest-asyncio
```

### Kafka Integration

**Detect**: kafka in dependencies, KafkaTemplate, @KafkaListener

**Key context:**
- Serialization (Avro, JSON, Protobuf)
- Consumer group strategy
- Error handling (DLT, retry)
- Idempotency approach

**Enhanced template:**
```markdown
## 구현 범위

### Producer
- Topic: [topic-name]
- Key strategy: [partitioning logic]
- Serializer: [JSON/Avro]
- 멱등성: enable.idempotence=true

### Consumer
- Group ID: [consumer-group]
- Concurrency: [partition count 기반]
- Offset commit: [manual/auto]
- Error handling: [retry + DLT]

### Schema
- Event: [package]/event/[Name]Event.java
- Avro schema: [if applicable]

### Configuration
- Producer config: acks, retries, batch.size
- Consumer config: max.poll.records, session.timeout

## 성공 기준
✅ At-least-once delivery 보장
✅ 멱등성 처리 (idempotent consumer)
✅ DLT(Dead Letter Topic) 구성
✅ 메시지 순서 보장 (if required)
✅ Consumer lag 모니터링 메트릭
✅ 통합 테스트 (EmbeddedKafka/@Testcontainers)
```

### Kubernetes Deployment

**Detect**: k8s/, manifests/, helm/, Dockerfile

**Key context:**
- Existing resource patterns
- ConfigMap/Secret usage
- Service mesh (Istio)
- HPA/VPA settings

**Enhanced template:**
```markdown
## 인프라 요구사항

### Kubernetes Resources
- Deployment: [replicas, resource limits]
- Service: [ClusterIP/LoadBalancer]
- ConfigMap: [환경 설정]
- Secret: [민감 정보]

### Helm Chart (if applicable)
- values.yaml: [환경별 설정]
- templates/: [resource templates]

### Health & Observability
- Liveness probe: /actuator/health/liveness
- Readiness probe: /actuator/health/readiness
- Prometheus metrics: /actuator/prometheus

### Scaling
- HPA: [CPU/Memory threshold]
- Resource requests/limits

## 성공 기준
✅ Zero-downtime deployment (RollingUpdate)
✅ Graceful shutdown 처리
✅ ConfigMap 외부화
✅ Resource limits 설정
✅ Health check endpoints
```

### Batch Processing (Spring Batch / Airflow)

**Detect**: spring-batch, airflow DAGs, @Scheduled

**Key context:**
- Job/Step structure
- Chunk vs Tasklet
- Error recovery strategy
- Scheduling approach

**Enhanced template (Spring Batch):**
```markdown
## 구현 범위

### Job Configuration
- Job: [Name]BatchJob
- Steps: [step flow]
- Parameters: [job parameters]

### Step Implementation
- Reader: [JdbcPagingItemReader/etc]
- Processor: [transformation logic]
- Writer: [JdbcBatchItemWriter/etc]
- Chunk size: [optimal size]

### Error Handling
- Skip policy: [skippable exceptions]
- Retry policy: [retryable exceptions]
- Restart: [job restart strategy]

### Monitoring
- JobExecutionListener: 시작/종료 로깅
- StepExecutionListener: step 메트릭

## 성공 기준
✅ Chunk 기반 처리 (메모리 효율)
✅ 실패 시 재시작 가능
✅ 처리 건수/시간 메트릭
✅ 대용량 처리 성능 (목표 TPS)
```

**Enhanced template (Airflow):**
```markdown
## 구현 범위

### DAG Configuration
- DAG ID: [name]
- Schedule: [cron expression]
- Catchup: [True/False]

### Tasks
- Task 1: [Operator type] - [description]
- Task 2: [Operator type] - [description]
- Dependencies: task1 >> task2

### Error Handling
- Retries: [count]
- Retry delay: [interval]
- On failure callback: [alert]

### XCom / Variables
- [Data passing strategy]

## 성공 기준
✅ Task 멱등성 보장
✅ 적절한 task 분리 (SRP)
✅ 모니터링 alert 설정
✅ Backfill 가능
```

### Database Operations

**Detect**: Entity classes, Repository, migration scripts

**Key context:**
- ORM vs Native query usage
- Transaction isolation level
- Batch operation patterns
- Index strategy

**Enhanced template:**
```markdown
## 구현 범위

### Schema Changes
- Table: [table_name]
- Columns: [new/modified columns]
- Indexes: [index strategy]
- Migration: [Flyway/Liquibase script]

### Data Access
- Entity: [Entity class]
- Repository: [Repository interface]
- Query method: [JPA/MyBatis/Native]

### Performance Considerations
- Batch size: [for bulk operations]
- Fetch strategy: [LAZY/EAGER]
- N+1 prevention: [JOIN FETCH/EntityGraph]

### Transaction
- Isolation level: [if non-default]
- Propagation: [if specific]

## 성공 기준
✅ 인덱스 설계 (실행계획 확인)
✅ 대용량 처리 시 batch 처리
✅ Connection pool 설정 적정성
✅ Slow query 방지
```

---

## Example Enhancement

**User input:**
```
주문 이벤트 처리하는 consumer 만들어줘
```

**Enhanced output:**
```markdown
# 주문 이벤트 Consumer 구현 요구사항

## 📋 프로젝트 컨텍스트
- Stack: Java 17, Spring Boot 3.2
- Architecture: Hexagonal Architecture
- Messaging: Kafka 4.1.1 (Apache)
- Serialization: JSON (Jackson)
- Database: Oracle 19c
- Infra: Kubernetes

## 🎯 구현 범위

### 주요 기능
1. order-events 토픽에서 주문 이벤트 소비
2. 주문 상태 업데이트 처리
3. 실패 시 DLT로 전송 및 알림

### 모듈 구조
```
com.example.order/
├── adapter/
│   └── in/
│       └── kafka/
│           ├── OrderEventConsumer.java
│           └── OrderEventMessage.java
├── application/
│   ├── port/in/ProcessOrderEventUseCase.java
│   └── service/OrderEventService.java
├── domain/
│   ├── Order.java
│   └── OrderStatus.java
└── config/
    └── KafkaConsumerConfig.java
```

## 📝 상세 요구사항

### 1. Kafka Consumer
- **위치**: adapter/in/kafka/OrderEventConsumer.java
- **목적**: order-events 토픽 메시지 소비
- **구현 내용**:
  - @KafkaListener with containerFactory
  - Consumer group: order-service-group
  - Concurrency: 3 (파티션 수 기반)
  - Manual ack: Acknowledgment.acknowledge()
  - Error handler: DefaultErrorHandler with DLT
- **기존 패턴**: PaymentEventConsumer와 동일 구조

### 2. Event Message
- **위치**: adapter/in/kafka/OrderEventMessage.java
- **목적**: Kafka 메시지 역직렬화
- **구현 내용**:
  - orderId, status, timestamp, payload
  - @JsonIgnoreProperties(ignoreUnknown = true)
  - Record 또는 Immutable class

### 3. Application Service
- **위치**: application/service/OrderEventService.java
- **목적**: 이벤트 처리 비즈니스 로직
- **구현 내용**:
  - ProcessOrderEventUseCase 구현
  - @Transactional 적용
  - 멱등성 체크 (processed_events 테이블)
  - 상태별 처리 로직 분기
- **기존 패턴**: 다른 EventService와 동일

### 4. Domain Entity 수정
- **위치**: domain/Order.java
- **목적**: 주문 상태 변경 로직
- **구현 내용**:
  - updateStatus(OrderStatus) 메서드
  - 상태 전이 검증 로직
  - Domain event 발행 (if needed)

### 5. Error Handling
- **위치**: config/KafkaConsumerConfig.java
- **목적**: Consumer 에러 처리 설정
- **구현 내용**:
  - DefaultErrorHandler 설정
  - FixedBackOff(1000L, 3) 재시도
  - DeadLetterPublishingRecoverer → order-events.DLT
  - RetryableTopic (optional)

### 6. Idempotency
- **위치**: application/service/OrderEventService.java
- **목적**: 중복 처리 방지
- **구현 내용**:
  - processed_events 테이블 체크
  - eventId + consumer 조합 unique
  - SELECT FOR UPDATE or INSERT IGNORE

## ⚙️ 인프라 요구사항

### Kafka Topics
- Input: order-events (existing)
- DLT: order-events.DLT (신규 생성 필요)
- Partitions: 3
- Replication factor: 3

### Database
```sql
CREATE TABLE processed_events (
    event_id VARCHAR(36) PRIMARY KEY,
    consumer_name VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_event_consumer UNIQUE (event_id, consumer_name)
);
```

### Monitoring
- Consumer lag metric: kafka_consumer_lag
- Processing time: order_event_processing_seconds
- Error count: order_event_errors_total

## ✅ 성공 기준
- [ ] order-events 토픽 메시지 정상 소비
- [ ] 주문 상태 DB 업데이트
- [ ] 중복 메시지 멱등성 처리
- [ ] 3회 재시도 후 DLT 전송
- [ ] Consumer lag 모니터링 가능
- [ ] 기존 아키텍처 패턴 일관성 유지
- [ ] 단위 테스트 (Mockito)
- [ ] 통합 테스트 (@EmbeddedKafka)

## 🔍 확인 사항
- order-events 토픽의 메시지 스키마가 확정되었나요?
- Consumer group은 order-service-group이 맞나요?
- DLT 메시지 재처리 정책이 있나요? (수동/자동)
- 처리 실패 시 알림(Slack/PagerDuty) 연동이 필요한가요?

---
이 요구사항으로 진행할까요? 수정이 필요한 부분이 있다면 말씀해주세요!
```

---

## Tips

### Infrastructure Dependencies

```markdown
## 🔗 인프라 의존성
- Kafka cluster: [broker addresses]
- Oracle DB: [connection info]
- Redis cache: [if applicable]
- External API: [endpoints]
```

### Non-Functional Requirements

Always consider:
```markdown
## ⚡ NFR 고려사항
- 예상 TPS: [target throughput]
- Latency 목표: [p99 latency]
- 데이터 보존: [retention policy]
- 장애 복구: [recovery strategy]
```

### Reference Files

- **Enhancement patterns**: references/enhancement-patterns.md
- **Architecture guides**: references/architecture-guides.md
- **Stack templates**: references/stack-templates.md
