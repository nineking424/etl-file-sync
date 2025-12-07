# Architecture Decision Records (ADR)

ADR은 아키텍처 의사결정을 문서화하여 컨텍스트, 대안, 결정 근거, 결과를 명확히 기록하는 방법입니다.

## ADR이 필요한 이유

1. **의사결정 컨텍스트 보존**: 왜 이 결정을 했는지 기록
2. **팀 커뮤니케이션**: 새로운 팀원이 빠르게 이해
3. **변경 추적**: 아키텍처 진화 과정 추적
4. **책임 명확화**: 누가, 언제, 왜 결정했는지 기록

## ADR 템플릿

### 기본 템플릿 (Michael Nygard)

```markdown
# ADR-{번호}: {제목}

## Status
{Proposed | Accepted | Deprecated | Superseded by ADR-XXX}

## Context
{의사결정이 필요한 배경과 상황 설명}

## Decision
{선택한 솔루션과 그 이유}

## Consequences
{이 결정의 긍정적/부정적 결과}
```

### 확장 템플릿 (MADR)

```markdown
# ADR-{번호}: {제목}

## Status
{Proposed | Accepted | Deprecated | Superseded}

## Context and Problem Statement
{해결하려는 문제와 배경}

## Decision Drivers
{의사결정에 영향을 준 요인들}
* Driver 1
* Driver 2

## Considered Options
{고려한 대안들}
* Option 1
* Option 2
* Option 3

## Decision Outcome
{선택한 옵션과 이유}

Chosen option: "{선택한 옵션}", because {이유}.

### Positive Consequences
{긍정적 결과}

### Negative Consequences
{부정적 결과}

## Pros and Cons of the Options

### {Option 1}
* Good, because {이유}
* Bad, because {이유}

### {Option 2}
* Good, because {이유}
* Bad, because {이유}

## Links
{관련 ADR, 문서, 이슈 링크}
```

## 실제 ADR 예제

### 예제 1: 데이터베이스 선택

```markdown
# ADR-001: PostgreSQL을 주 데이터베이스로 선택

## Status
Accepted

## Context and Problem Statement
새로운 주문 관리 시스템을 구축하면서 데이터베이스를 선택해야 합니다.

주요 요구사항:
- ACID 트랜잭션 보장 필요
- 복잡한 조인 쿼리 지원
- JSON 데이터 저장 필요
- 오픈소스 선호
- 확장성 고려

## Decision Drivers
* 트랜잭션 무결성이 중요
* 관계형 데이터 모델이 적합
* 팀의 SQL 경험 많음
* 비용 절감 (오픈소스)
* 클라우드 호환성

## Considered Options
* PostgreSQL
* MySQL
* MongoDB
* Amazon Aurora

## Decision Outcome
Chosen option: "PostgreSQL", because:
- 강력한 ACID 트랜잭션 지원
- JSONB 타입으로 유연한 스키마 지원
- 우수한 성능과 확장성
- 활발한 커뮤니티
- AWS RDS 지원으로 관리 용이

### Positive Consequences
* 데이터 무결성 보장
* 복잡한 쿼리 성능 우수
* JSON 데이터 효율적 저장
* 무료 오픈소스

### Negative Consequences
* NoSQL 대비 수평 확장 제한적
* 초기 설정 및 튜닝 필요
* 대용량 데이터 처리 시 샤딩 복잡

## Pros and Cons of the Options

### PostgreSQL (선택)
* Good: ACID 보장, JSONB 지원, 확장 기능 풍부
* Good: 활발한 커뮤니티, 안정성
* Bad: 수평 확장 복잡
* Bad: 운영 노하우 필요

### MySQL
* Good: 널리 사용됨, 간단한 설정
* Good: 읽기 성능 우수
* Bad: JSON 지원 제한적
* Bad: 일부 기능 제약

### MongoDB
* Good: 유연한 스키마, 수평 확장 용이
* Bad: ACID 트랜잭션 제한적
* Bad: 복잡한 조인 어려움

### Amazon Aurora
* Good: 관리형 서비스, 자동 확장
* Good: 높은 가용성
* Bad: 벤더 종속성
* Bad: 비용 높음

## Links
* [PostgreSQL Documentation](https://www.postgresql.org/docs/)
* Related: ADR-005 (Database Sharding Strategy)
```

### 예제 2: 아키텍처 패턴 선택

```markdown
# ADR-002: 헥사고날 아키텍처 채택

## Status
Accepted

## Context
주문 관리 마이크로서비스를 개발하면서 아키텍처 패턴을 선택해야 합니다.

요구사항:
- 비즈니스 로직과 인프라 분리
- 외부 시스템 통합 많음 (결제, 배송, 알림)
- 테스트 가능성 중요
- 향후 외부 시스템 교체 가능성

## Decision Drivers
* 외부 시스템 의존성이 높음
* 비즈니스 로직의 독립성 필요
* 단위 테스트 및 통합 테스트 용이성
* 유지보수성

## Considered Options
* Layered Architecture
* Hexagonal Architecture (Ports & Adapters)
* Clean Architecture

## Decision Outcome
Chosen option: "Hexagonal Architecture", because:
- 비즈니스 로직을 인프라로부터 완전히 분리
- Ports(인터페이스)를 통한 의존성 역전
- 외부 시스템 교체 시 Adapter만 변경
- Mock 객체를 통한 테스트 용이

### Positive Consequences
* 비즈니스 로직 단위 테스트 용이
* 외부 시스템 교체 영향 최소화
* 도메인 중심 설계 가능
* 명확한 책임 분리

### Negative Consequences
* 초기 개발 시간 증가 (인터페이스, 구현체 분리)
* 코드량 증가
* 팀원 학습 곡선
* 과도한 추상화 위험

### Mitigation
* 팀 교육 세션 진행
* 샘플 코드 및 가이드 작성
* 코드 리뷰로 과도한 추상화 방지

## Pros and Cons of the Options

### Layered Architecture
* Good: 단순하고 이해하기 쉬움
* Good: 빠른 개발 가능
* Bad: 레이어 간 강한 결합
* Bad: 테스트 어려움
* Bad: 외부 시스템 교체 어려움

### Hexagonal Architecture (선택)
* Good: 비즈니스 로직 독립성
* Good: 테스트 용이성
* Good: 외부 시스템 교체 용이
* Bad: 초기 복잡도 증가
* Bad: 코드량 증가

### Clean Architecture
* Good: 강력한 의존성 규칙
* Good: 프레임워크 독립성
* Bad: 복잡도가 매우 높음
* Bad: 학습 곡선 가파름
* Bad: 오버엔지니어링 위험

## Implementation Notes

### Package Structure
```
src/
├── domain/          # 비즈니스 로직
│   ├── model/
│   ├── service/
│   └── port/        # 인터페이스
├── application/     # Use Cases
└── adapter/         # 어댑터
    ├── in/          # Inbound (REST, CLI)
    └── out/         # Outbound (DB, External API)
```

## Links
* [Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture/)
* Related: ADR-003 (DDD Tactical Patterns)
```

### 예제 3: 기술 스택 선택

```markdown
# ADR-003: Kafka를 이벤트 버스로 선택

## Status
Accepted

## Context
마이크로서비스 간 비동기 통신을 위한 이벤트 버스가 필요합니다.

요구사항:
- 높은 처리량 (초당 수만 건)
- 메시지 순서 보장
- 이벤트 리플레이 가능
- At-least-once 전송 보장
- 확장성

## Decision Drivers
* 높은 처리량 요구
* 이벤트 소싱 구현 예정
* 여러 컨슈머가 같은 이벤트 구독
* 이벤트 히스토리 유지 필요

## Considered Options
* Apache Kafka
* RabbitMQ
* Amazon SQS
* Redis Streams

## Decision Outcome
Chosen option: "Apache Kafka", because:
- 초당 수백만 건 처리 가능
- 파티션을 통한 순서 보장
- 이벤트 리플레이 지원 (retention)
- 수평 확장 용이
- 이벤트 소싱에 적합

### Positive Consequences
* 높은 처리량
* 이벤트 히스토리 유지
* 여러 컨슈머 독립적 처리
* 장애 복구 용이

### Negative Consequences
* 운영 복잡도 증가 (ZooKeeper 관리)
* 작은 메시지에는 오버헤드
* 학습 곡선
* 인프라 비용

## Pros and Cons of the Options

### Apache Kafka (선택)
* Good: 매우 높은 처리량
* Good: 이벤트 리플레이
* Good: 파티션 기반 확장
* Bad: 운영 복잡
* Bad: 지연시간이 다소 높음

### RabbitMQ
* Good: 사용하기 쉬움
* Good: 낮은 지연시간
* Bad: 처리량 제한적
* Bad: 이벤트 리플레이 불가

### Amazon SQS
* Good: 관리형 서비스
* Good: 간단한 사용
* Bad: 순서 보장 제한적
* Bad: 벤더 종속

### Redis Streams
* Good: 매우 빠름
* Good: 간단한 설정
* Bad: 영속성 제한적
* Bad: 확장성 제한

## Migration Plan
1. Phase 1: Kafka 클러스터 구축 (개발 환경)
2. Phase 2: 주문 이벤트 마이그레이션
3. Phase 3: 나머지 이벤트 순차 마이그레이션
4. Phase 4: 기존 메시징 시스템 폐기

## Links
* [Kafka Documentation](https://kafka.apache.org/documentation/)
* Related: ADR-004 (Event Sourcing Pattern)
```

## ADR 관리 모범 사례

### 1. 파일 구조

```
docs/adr/
├── README.md
├── template.md
├── 0001-postgresql-database.md
├── 0002-hexagonal-architecture.md
├── 0003-kafka-event-bus.md
└── 0004-superseded-by-0005.md
```

### 2. 네이밍 규칙

```
{번호}-{짧은-제목}.md

예:
0001-use-postgresql.md
0002-adopt-hexagonal-architecture.md
0003-choose-kafka-for-events.md
```

### 3. Status 변경

```markdown
# ADR-003: RabbitMQ for Message Queue

## Status
~~Accepted~~ Superseded by ADR-007

## Superseded By
This decision has been superseded by ADR-007 which adopts Kafka for better scalability.
```

### 4. ADR 인덱스

```markdown
# Architecture Decision Records

## Active
* [ADR-001](0001-postgresql-database.md) - Use PostgreSQL as primary database
* [ADR-002](0002-hexagonal-architecture.md) - Adopt Hexagonal Architecture
* [ADR-007](0007-kafka-event-bus.md) - Use Kafka for event streaming

## Superseded
* [ADR-003](0003-rabbitmq-queue.md) - Use RabbitMQ (superseded by ADR-007)

## Deprecated
* [ADR-005](0005-mongodb-cache.md) - Use MongoDB for caching
```

## ADR 작성 시점

다음 상황에서 ADR을 작성하세요:

1. **아키텍처 패턴 선택**
   - Layered vs Hexagonal vs Clean Architecture

2. **기술 스택 선택**
   - 데이터베이스, 메시징 시스템, 캐시 등

3. **배포 전략**
   - Blue-Green, Canary, Rolling Update

4. **보안 메커니즘**
   - 인증/인가 방식

5. **데이터 관리**
   - 데이터베이스 스키마, 샤딩 전략

6. **통합 방식**
   - API Gateway, Service Mesh

7. **모니터링 및 로깅**
   - 도구 선택, 로깅 전략

## 체크리스트

- [ ] 제목은 명확하고 구체적인가?
- [ ] 컨텍스트를 충분히 설명했는가?
- [ ] 고려한 대안을 나열했는가?
- [ ] 선택 이유를 명확히 기술했는가?
- [ ] 긍정적/부정적 결과를 모두 문서화했는가?
- [ ] 관련 ADR과 링크했는가?
- [ ] Status가 명확한가?
- [ ] 팀 리뷰를 받았는가?
