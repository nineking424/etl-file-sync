---
name: api-designer
description: Design RESTful APIs and gRPC services following industry best practices with OpenAPI specification generation and versioning strategies
---

# API Designer Skill

RESTful API와 gRPC 서비스를 업계 모범 사례에 따라 설계하고, OpenAPI 명세를 생성하며, API 버저닝 전략을 수립합니다.

## Overview

API Designer는 확장 가능하고 유지보수 가능한 API를 설계하기 위한 전문 스킬입니다. RESTful 원칙, OpenAPI 3.0 명세 생성, gRPC 프로토콜 버퍼 정의, API 버저닝 전략을 포함한 포괄적인 API 설계 솔루션을 제공합니다.

## Core Capabilities

### 1. RESTful API 설계
- HTTP 메서드 올바른 사용 (GET, POST, PUT, PATCH, DELETE)
- 리소스 기반 URL 설계
- 상태 코드 적절한 활용
- HATEOAS (Hypermedia as the Engine of Application State) 적용
- 일관된 응답 구조 설계

### 2. OpenAPI 3.0 명세 생성
- 완전한 API 문서화
- 요청/응답 스키마 정의
- 예제 데이터 포함
- 보안 스키마 정의
- 인터랙티브 API 문서 생성

### 3. gRPC 서비스 설계
- Protocol Buffers (proto3) 정의
- 서비스 메서드 설계
- 메시지 타입 정의
- 스트리밍 API 설계 (단방향/양방향)
- 에러 핸들링 패턴

### 4. API 버저닝 전략
- URI 버저닝 (e.g., /v1/, /v2/)
- 헤더 버저닝 (Accept header)
- 쿼리 파라미터 버저닝
- 하위 호환성 유지
- 버전 폐기(deprecation) 전략

### 5. 페이지네이션 및 필터링
- 오프셋 기반 페이지네이션
- 커서 기반 페이지네이션
- 필터링 쿼리 파라미터
- 정렬 및 검색 기능
- 메타데이터 응답 설계

### 6. 에러 응답 표준화
- RFC 7807 Problem Details
- 일관된 에러 구조
- 에러 코드 체계
- 상세한 에러 메시지
- 디버깅 정보 포함

## Workflow

### 1. 요구사항 분석
- 비즈니스 도메인 이해
- 리소스 식별
- 엔드포인트 필요성 파악
- 데이터 모델 정의
- 인증/인가 요구사항 확인

### 2. API 설계
- URL 구조 설계
- HTTP 메서드 선택
- 요청/응답 형식 정의
- 에러 처리 방식 결정
- 버저닝 전략 수립

### 3. 명세 작성
- OpenAPI/gRPC proto 파일 생성
- 스키마 정의
- 예제 추가
- 보안 설정
- 문서화

### 4. 검증 및 리뷰
- API 일관성 검증
- RESTful 원칙 준수 확인
- 보안 취약점 검토
- 성능 고려사항 확인
- 팀 리뷰 및 피드백 반영

### 5. 문서화 및 배포
- 인터랙티브 문서 생성
- API 가이드 작성
- 변경 사항 기록
- 클라이언트 SDK 생성 고려
- API 게이트웨이 설정

## Checklist

### RESTful API 설계
- [ ] 리소스 중심의 URL 구조 사용
- [ ] 적절한 HTTP 메서드 활용
- [ ] 올바른 상태 코드 반환
- [ ] 일관된 네이밍 컨벤션 적용
- [ ] HATEOAS 링크 포함 고려

### OpenAPI 명세
- [ ] OpenAPI 3.0 스펙 준수
- [ ] 모든 엔드포인트 문서화
- [ ] 요청/응답 스키마 정의
- [ ] 예제 데이터 포함
- [ ] 보안 스키마 정의

### 데이터 설계
- [ ] 일관된 응답 구조
- [ ] 적절한 데이터 타입 사용
- [ ] 필수/선택 필드 명시
- [ ] 검증 규칙 정의
- [ ] 날짜/시간 형식 표준화 (ISO 8601)

### 에러 처리
- [ ] 표준화된 에러 형식
- [ ] 명확한 에러 메시지
- [ ] 적절한 HTTP 상태 코드
- [ ] 에러 코드 체계 구축
- [ ] 디버깅 정보 제공

### 버저닝
- [ ] 버저닝 전략 선택
- [ ] 하위 호환성 고려
- [ ] 변경 사항 문서화
- [ ] 폐기 정책 수립
- [ ] 마이그레이션 가이드 제공

### 보안
- [ ] 인증 메커니즘 정의
- [ ] 인가 정책 수립
- [ ] HTTPS 필수 사용
- [ ] Rate limiting 고려
- [ ] CORS 정책 설정

## Usage Examples

### Example 1: RESTful API 설계

**요청:** "사용자 관리 API를 설계해주세요"

**생성된 OpenAPI 명세:**

```yaml
openapi: 3.0.3
info:
  title: User Management API
  version: 1.0.0
  description: API for managing user resources

servers:
  - url: https://api.example.com/v1
    description: Production server

paths:
  /users:
    get:
      summary: List users
      operationId: listUsers
      tags:
        - Users
      parameters:
        - name: page
          in: query
          schema:
            type: integer
            default: 1
        - name: limit
          in: query
          schema:
            type: integer
            default: 20
            maximum: 100
      responses:
        '200':
          description: Successful response
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UserListResponse'

    post:
      summary: Create a new user
      operationId: createUser
      tags:
        - Users
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CreateUserRequest'
      responses:
        '201':
          description: User created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/User'
        '400':
          description: Invalid input
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'

  /users/{userId}:
    get:
      summary: Get user by ID
      operationId: getUserById
      tags:
        - Users
      parameters:
        - name: userId
          in: path
          required: true
          schema:
            type: string
            format: uuid
      responses:
        '200':
          description: Successful response
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/User'
        '404':
          description: User not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'

components:
  schemas:
    User:
      type: object
      required:
        - id
        - email
        - name
      properties:
        id:
          type: string
          format: uuid
        email:
          type: string
          format: email
        name:
          type: string
        createdAt:
          type: string
          format: date-time

    CreateUserRequest:
      type: object
      required:
        - email
        - name
      properties:
        email:
          type: string
          format: email
        name:
          type: string
          minLength: 1
          maxLength: 100

    UserListResponse:
      type: object
      properties:
        data:
          type: array
          items:
            $ref: '#/components/schemas/User'
        pagination:
          $ref: '#/components/schemas/Pagination'

    Pagination:
      type: object
      properties:
        page:
          type: integer
        limit:
          type: integer
        total:
          type: integer
        totalPages:
          type: integer

    Error:
      type: object
      required:
        - type
        - title
        - status
      properties:
        type:
          type: string
        title:
          type: string
        status:
          type: integer
        detail:
          type: string
        instance:
          type: string

  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT

security:
  - bearerAuth: []
```

### Example 2: gRPC 서비스 설계

**요청:** "주문 관리를 위한 gRPC 서비스를 설계해주세요"

**생성된 Proto 파일:**

```protobuf
syntax = "proto3";

package order.v1;

import "google/protobuf/timestamp.proto";

option go_package = "github.com/example/order/v1;orderv1";

// Order service for managing customer orders
service OrderService {
  // Creates a new order
  rpc CreateOrder(CreateOrderRequest) returns (Order);

  // Gets an order by ID
  rpc GetOrder(GetOrderRequest) returns (Order);

  // Lists orders with pagination
  rpc ListOrders(ListOrdersRequest) returns (ListOrdersResponse);

  // Updates an existing order
  rpc UpdateOrder(UpdateOrderRequest) returns (Order);

  // Cancels an order
  rpc CancelOrder(CancelOrderRequest) returns (Order);

  // Streams order status updates
  rpc WatchOrderStatus(WatchOrderStatusRequest) returns (stream OrderStatusUpdate);
}

message Order {
  string id = 1;
  string customer_id = 2;
  OrderStatus status = 3;
  repeated OrderItem items = 4;
  double total_amount = 5;
  google.protobuf.Timestamp created_at = 6;
  google.protobuf.Timestamp updated_at = 7;
}

message OrderItem {
  string product_id = 1;
  int32 quantity = 2;
  double unit_price = 3;
}

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_PENDING = 1;
  ORDER_STATUS_CONFIRMED = 2;
  ORDER_STATUS_PROCESSING = 3;
  ORDER_STATUS_SHIPPED = 4;
  ORDER_STATUS_DELIVERED = 5;
  ORDER_STATUS_CANCELLED = 6;
}

message CreateOrderRequest {
  string customer_id = 1;
  repeated OrderItem items = 2;
}

message GetOrderRequest {
  string id = 1;
}

message ListOrdersRequest {
  string customer_id = 1;
  int32 page_size = 2;
  string page_token = 3;
}

message ListOrdersResponse {
  repeated Order orders = 1;
  string next_page_token = 2;
}

message UpdateOrderRequest {
  string id = 1;
  OrderStatus status = 2;
}

message CancelOrderRequest {
  string id = 1;
  string reason = 2;
}

message WatchOrderStatusRequest {
  string id = 1;
}

message OrderStatusUpdate {
  string order_id = 1;
  OrderStatus old_status = 2;
  OrderStatus new_status = 3;
  google.protobuf.Timestamp timestamp = 4;
}
```

## Integration Points

### 다른 스킬과의 연계

- **architecture-advisor**: API 아키텍처 패턴 자문
- **developer**: API 구현 코드 생성
- **tester**: API 테스트 케이스 작성
- **document-maintainer**: API 문서 유지보수
- **code-reviewer**: API 설계 리뷰

### 외부 도구 통합

- **Swagger UI**: 인터랙티브 API 문서
- **Postman**: API 테스팅 컬렉션
- **API Gateway**: Kong, AWS API Gateway, Azure APIM
- **gRPC Tools**: grpcurl, BloomRPC
- **OpenAPI Generator**: 클라이언트 SDK 생성

## References

- `references/rest-best-practices.md` - RESTful API 설계 모범 사례
- `references/api-versioning.md` - API 버저닝 전략
- `references/error-response-patterns.md` - 에러 응답 패턴
- `references/pagination-patterns.md` - 페이지네이션 패턴

## Best Practices

1. **일관성 유지**: 모든 엔드포인트에서 일관된 패턴 사용
2. **명확한 네이밍**: 직관적이고 설명적인 리소스 이름
3. **적절한 HTTP 메서드**: 의미에 맞는 메서드 선택
4. **상세한 문서화**: 모든 엔드포인트와 파라미터 문서화
5. **보안 우선**: 인증, 인가, HTTPS 필수
6. **버저닝**: 초기부터 버저닝 전략 수립
7. **에러 처리**: 명확하고 일관된 에러 응답
8. **테스트 가능성**: API 테스트 용이하도록 설계
