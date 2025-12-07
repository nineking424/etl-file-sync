# RESTful API 설계 모범 사례

이 문서는 확장 가능하고 유지보수 가능한 RESTful API를 설계하기 위한 모범 사례를 제공합니다.

## REST 핵심 원칙

### 1. 리소스 중심 설계

API는 리소스를 중심으로 설계되어야 합니다. 리소스는 명사로 표현되며, 동사는 HTTP 메서드로 표현됩니다.

```
좋은 예:
GET    /users          # 사용자 목록 조회
POST   /users          # 사용자 생성
GET    /users/123      # 특정 사용자 조회
PUT    /users/123      # 사용자 전체 업데이트
PATCH  /users/123      # 사용자 부분 업데이트
DELETE /users/123      # 사용자 삭제

나쁜 예:
GET    /getUsers
POST   /createUser
GET    /user/get/123
POST   /deleteUser/123
```

### 2. HTTP 메서드 올바른 사용

각 HTTP 메서드는 명확한 의미를 가집니다:

- **GET**: 리소스 조회 (멱등성, 안전)
- **POST**: 리소스 생성
- **PUT**: 리소스 전체 교체 (멱등성)
- **PATCH**: 리소스 부분 수정 (멱등성)
- **DELETE**: 리소스 삭제 (멱등성)

```yaml
# 멱등성 예제
PUT /users/123
{
  "name": "John Doe",
  "email": "john@example.com"
}
# 여러 번 호출해도 결과 동일

PATCH /users/123
{
  "email": "newemail@example.com"
}
# 여러 번 호출해도 결과 동일
```

### 3. 적절한 HTTP 상태 코드 사용

상태 코드는 응답의 성격을 명확히 전달합니다:

**2xx - 성공**
- `200 OK`: 성공 (GET, PUT, PATCH)
- `201 Created`: 생성 성공 (POST)
- `204 No Content`: 성공, 응답 본문 없음 (DELETE)

**3xx - 리다이렉션**
- `301 Moved Permanently`: 영구 이동
- `304 Not Modified`: 캐시된 버전 사용

**4xx - 클라이언트 에러**
- `400 Bad Request`: 잘못된 요청
- `401 Unauthorized`: 인증 필요
- `403 Forbidden`: 권한 없음
- `404 Not Found`: 리소스 없음
- `409 Conflict`: 충돌 (중복 생성 등)
- `422 Unprocessable Entity`: 검증 실패

**5xx - 서버 에러**
- `500 Internal Server Error`: 서버 오류
- `503 Service Unavailable`: 서비스 이용 불가

## URL 설계 패턴

### 1. 계층 구조 표현

```
/organizations/{orgId}/teams/{teamId}/members
/projects/{projectId}/tasks/{taskId}/comments
/customers/{customerId}/orders/{orderId}/items
```

### 2. 복수형 명사 사용

```
좋은 예:
/users
/products
/orders

나쁜 예:
/user
/product
/order
```

### 3. 케밥 케이스 사용

```
좋은 예:
/user-profiles
/order-items
/payment-methods

나쁜 예:
/userProfiles
/order_items
/PaymentMethods
```

### 4. 쿼리 파라미터 활용

```
# 필터링
GET /products?category=electronics&price_min=100&price_max=500

# 정렬
GET /users?sort=created_at:desc,name:asc

# 검색
GET /articles?q=rest+api&author=john

# 필드 선택
GET /users?fields=id,name,email

# 페이지네이션
GET /items?page=2&limit=20
```

## 요청 및 응답 형식

### 1. JSON 표준 사용

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "name": "John Doe",
  "created_at": "2025-01-15T10:30:00Z",
  "is_active": true,
  "profile": {
    "bio": "Software Developer",
    "location": "Seoul, Korea"
  },
  "tags": ["developer", "backend", "api"]
}
```

### 2. 일관된 응답 구조

```json
// 단일 리소스
{
  "data": {
    "id": "123",
    "name": "Product Name"
  }
}

// 리소스 목록
{
  "data": [
    {"id": "1", "name": "Item 1"},
    {"id": "2", "name": "Item 2"}
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 100,
    "total_pages": 5
  }
}

// 에러 응답
{
  "error": {
    "type": "validation_error",
    "title": "Validation Failed",
    "status": 422,
    "detail": "The request body is invalid",
    "errors": [
      {
        "field": "email",
        "message": "Email is required"
      }
    ]
  }
}
```

### 3. 네이밍 컨벤션

```json
// snake_case 사용 (권장)
{
  "user_id": "123",
  "created_at": "2025-01-15T10:30:00Z",
  "is_active": true
}

// 또는 camelCase 사용 (일관성 유지)
{
  "userId": "123",
  "createdAt": "2025-01-15T10:30:00Z",
  "isActive": true
}
```

## HATEOAS (Hypermedia)

클라이언트가 다음 작업을 발견할 수 있도록 링크 제공:

```json
{
  "data": {
    "id": "123",
    "name": "John Doe",
    "email": "john@example.com"
  },
  "links": {
    "self": "/users/123",
    "orders": "/users/123/orders",
    "update": {
      "href": "/users/123",
      "method": "PUT"
    },
    "delete": {
      "href": "/users/123",
      "method": "DELETE"
    }
  }
}
```

## 페이지네이션 패턴

### 1. 오프셋 기반

```
GET /users?page=2&limit=20

응답:
{
  "data": [...],
  "pagination": {
    "page": 2,
    "limit": 20,
    "total": 100,
    "total_pages": 5
  },
  "links": {
    "first": "/users?page=1&limit=20",
    "prev": "/users?page=1&limit=20",
    "next": "/users?page=3&limit=20",
    "last": "/users?page=5&limit=20"
  }
}
```

### 2. 커서 기반

```
GET /posts?cursor=eyJpZCI6MTIzfQ==&limit=20

응답:
{
  "data": [...],
  "pagination": {
    "next_cursor": "eyJpZCI6MTQzfQ==",
    "has_more": true
  }
}
```

## 필터링 및 검색

### 1. 필터링

```
# 단일 필터
GET /products?category=electronics

# 다중 필터
GET /products?category=electronics&brand=samsung&in_stock=true

# 범위 필터
GET /products?price_min=100&price_max=500
```

### 2. 정렬

```
# 단일 정렬
GET /users?sort=created_at

# 역순 정렬
GET /users?sort=-created_at

# 다중 정렬
GET /users?sort=last_name,first_name
GET /users?sort=-created_at,name
```

### 3. 전체 텍스트 검색

```
GET /articles?q=rest+api+best+practices
GET /products?search=samsung+galaxy
```

## 버저닝 전략

### 1. URI 버저닝 (권장)

```
https://api.example.com/v1/users
https://api.example.com/v2/users
```

### 2. 헤더 버저닝

```
GET /users
Accept: application/vnd.company.v1+json

GET /users
Accept: application/vnd.company.v2+json
```

### 3. 쿼리 파라미터 버저닝

```
GET /users?version=1
GET /users?api-version=2
```

## 보안 모범 사례

### 1. HTTPS 필수

모든 API는 HTTPS를 통해서만 제공되어야 합니다.

### 2. 인증

```yaml
# Bearer Token (JWT)
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# API Key
X-API-Key: your-api-key-here

# Basic Auth (권장하지 않음)
Authorization: Basic base64(username:password)
```

### 3. Rate Limiting

```
HTTP/1.1 200 OK
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1640995200
```

### 4. CORS 설정

```
Access-Control-Allow-Origin: https://example.com
Access-Control-Allow-Methods: GET, POST, PUT, DELETE
Access-Control-Allow-Headers: Content-Type, Authorization
Access-Control-Max-Age: 86400
```

## 캐싱

### 1. ETag 사용

```
# 첫 요청
GET /users/123
ETag: "33a64df551425fcc55e4d42a148795d9f25f89d4"

# 조건부 요청
GET /users/123
If-None-Match: "33a64df551425fcc55e4d42a148795d9f25f89d4"

# 응답 (변경 없음)
HTTP/1.1 304 Not Modified
```

### 2. Cache-Control 헤더

```
Cache-Control: public, max-age=3600
Cache-Control: private, no-cache
Cache-Control: no-store
```

## 비동기 작업 처리

### 1. 긴 작업의 경우

```
# 작업 시작
POST /reports
{
  "type": "monthly_sales",
  "year": 2025,
  "month": 1
}

# 응답
HTTP/1.1 202 Accepted
Location: /reports/jobs/abc123

# 상태 확인
GET /reports/jobs/abc123
{
  "id": "abc123",
  "status": "processing",
  "progress": 45,
  "created_at": "2025-01-15T10:30:00Z"
}

# 완료 확인
GET /reports/jobs/abc123
{
  "id": "abc123",
  "status": "completed",
  "progress": 100,
  "result": {
    "url": "/reports/monthly-sales-2025-01.pdf"
  }
}
```

## 에러 처리

### RFC 7807 Problem Details 형식

```json
{
  "type": "https://example.com/errors/insufficient-balance",
  "title": "Insufficient Balance",
  "status": 400,
  "detail": "Your account balance is insufficient for this transaction",
  "instance": "/transactions/abc123",
  "balance": 50.00,
  "required": 100.00
}
```

## 체크리스트

- [ ] 리소스 중심의 URL 설계
- [ ] 복수형 명사 사용
- [ ] 적절한 HTTP 메서드 사용
- [ ] 올바른 상태 코드 반환
- [ ] 일관된 JSON 형식
- [ ] 페이지네이션 구현
- [ ] 필터링 및 정렬 지원
- [ ] 버저닝 전략 수립
- [ ] HTTPS 사용
- [ ] 인증/인가 구현
- [ ] Rate limiting 적용
- [ ] 에러 응답 표준화
- [ ] 캐싱 전략 수립
- [ ] CORS 설정
- [ ] API 문서화
