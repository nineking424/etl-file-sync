# API 버저닝 전략

API 버저닝은 기존 클라이언트에 영향을 주지 않으면서 API를 발전시키기 위한 핵심 전략입니다.

## 버저닝이 필요한 경우

### Breaking Changes (호환성 깨짐)

다음과 같은 변경사항은 새로운 버전이 필요합니다:

1. **엔드포인트 제거**
   ```
   DELETE /users/{id}  # 엔드포인트 완전 제거
   ```

2. **필수 필드 추가**
   ```json
   // v1 - phone은 선택
   {
     "name": "John",
     "email": "john@example.com"
   }

   // v2 - phone이 필수가 됨
   {
     "name": "John",
     "email": "john@example.com",
     "phone": "010-1234-5678"  // 필수!
   }
   ```

3. **응답 구조 변경**
   ```json
   // v1
   {
     "id": "123",
     "name": "John"
   }

   // v2 - 구조 변경
   {
     "user": {
       "id": "123",
       "name": "John"
     }
   }
   ```

4. **데이터 타입 변경**
   ```json
   // v1
   {
     "id": 123,  // integer
     "price": 99.99
   }

   // v2
   {
     "id": "123",  // string으로 변경
     "price": "99.99"  // string으로 변경
   }
   ```

### Non-Breaking Changes (호환성 유지)

다음 변경은 새 버전이 필요하지 않습니다:

1. **선택적 필드 추가**
2. **새로운 엔드포인트 추가**
3. **응답에 새 필드 추가**
4. **에러 메시지 개선**

## 버저닝 전략

### 1. URI 버저닝 (가장 일반적)

URL 경로에 버전을 포함합니다.

```
https://api.example.com/v1/users
https://api.example.com/v2/users
https://api.example.com/v3/users
```

**장점:**
- 명확하고 직관적
- 브라우저에서 쉽게 테스트 가능
- 캐싱이 용이
- API 게이트웨이에서 라우팅 쉬움

**단점:**
- URL이 지저분해질 수 있음
- 리소스 URI가 변경됨

**구현 예제:**

```yaml
# OpenAPI 3.0
openapi: 3.0.3
info:
  title: User API
  version: 2.0.0

servers:
  - url: https://api.example.com/v2
    description: Version 2

paths:
  /users:
    get:
      summary: List users (v2)
      responses:
        '200':
          description: Success
```

### 2. 헤더 버저닝

Accept 또는 커스텀 헤더를 통해 버전을 지정합니다.

```
GET /users
Accept: application/vnd.company.v1+json

GET /users
Accept: application/vnd.company.v2+json

GET /users
X-API-Version: 2
```

**장점:**
- URL이 깔끔함
- RESTful 원칙에 부합
- 세밀한 버전 제어 가능

**단점:**
- 브라우저에서 테스트 어려움
- 캐싱이 복잡해질 수 있음
- 클라이언트 구현이 복잡

**구현 예제:**

```javascript
// Express.js 미들웨어
app.use((req, res, next) => {
  const acceptHeader = req.get('Accept');

  if (acceptHeader && acceptHeader.includes('vnd.company.v2')) {
    req.apiVersion = 'v2';
  } else {
    req.apiVersion = 'v1';
  }

  next();
});

app.get('/users', (req, res) => {
  if (req.apiVersion === 'v2') {
    // v2 로직
    return res.json(getUsersV2());
  } else {
    // v1 로직
    return res.json(getUsersV1());
  }
});
```

### 3. 쿼리 파라미터 버저닝

쿼리 스트링에 버전을 지정합니다.

```
GET /users?version=1
GET /users?api-version=2
GET /users?v=3
```

**장점:**
- 구현이 간단
- 기존 URL 구조 유지

**단점:**
- RESTful 원칙에 부합하지 않음
- 쿼리 파라미터가 복잡해질 수 있음
- 덜 명확함

**구현 예제:**

```python
# Flask
@app.route('/users')
def get_users():
    version = request.args.get('version', '1')

    if version == '2':
        return jsonify(get_users_v2())
    else:
        return jsonify(get_users_v1())
```

### 4. Content Negotiation

Accept 헤더의 미디어 타입으로 버전 지정:

```
GET /users
Accept: application/json;version=1

GET /users
Accept: application/json;version=2
```

## 버전 관리 모범 사례

### 1. 시맨틱 버저닝

```
v1.0.0 - 초기 릴리스
v1.1.0 - 하위 호환성 유지하며 기능 추가
v1.1.1 - 버그 수정
v2.0.0 - Breaking change
```

**규칙:**
- Major: Breaking changes
- Minor: 하위 호환 기능 추가
- Patch: 버그 수정

### 2. 버전별 네임스페이스 분리

```
project/
├── api/
│   ├── v1/
│   │   ├── users.py
│   │   ├── products.py
│   │   └── orders.py
│   └── v2/
│       ├── users.py
│       ├── products.py
│       └── orders.py
```

### 3. 공통 코드 재사용

```python
# common/models.py
class User:
    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email

# api/v1/serializers.py
class UserSerializerV1:
    def serialize(self, user):
        return {
            "id": user.id,
            "name": user.name,
            "email": user.email
        }

# api/v2/serializers.py
class UserSerializerV2:
    def serialize(self, user):
        return {
            "data": {
                "id": user.id,
                "attributes": {
                    "name": user.name,
                    "email": user.email
                }
            }
        }
```

### 4. 기본 버전 설정

```javascript
// 버전 미지정 시 최신 안정 버전 사용
app.use((req, res, next) => {
  if (!req.apiVersion) {
    req.apiVersion = 'v2';  // 기본값
  }
  next();
});
```

## 버전 폐기(Deprecation) 전략

### 1. 폐기 공지

```yaml
# 헤더로 폐기 알림
GET /v1/users
HTTP/1.1 200 OK
Warning: 299 - "API version 1 is deprecated and will be removed on 2025-12-31"
Sunset: Sat, 31 Dec 2025 23:59:59 GMT
```

### 2. 폐기 타임라인

```
T+0개월: v2 릴리스, v1 deprecation 공지
T+3개월: v1 경고 추가, 문서 업데이트
T+6개월: v1 사용 시 deprecation 헤더 추가
T+9개월: v1 접근 제한 (제한적 사용만 허용)
T+12개월: v1 완전 제거
```

### 3. 폐기 문서화

```markdown
## Deprecated APIs

### /v1/users (deprecated)
**Deprecation Date:** 2025-01-15
**Sunset Date:** 2025-12-31
**Replacement:** /v2/users

**Migration Guide:**
- Response structure has changed
- Use `data` wrapper in v2
- See [migration guide](./migrations/v1-to-v2.md)
```

## 마이그레이션 지원

### 1. 마이그레이션 가이드 제공

```markdown
# v1 → v2 Migration Guide

## Breaking Changes

### 1. Response Structure
**v1:**
```json
{
  "id": "123",
  "name": "John"
}
```

**v2:**
```json
{
  "data": {
    "id": "123",
    "attributes": {
      "name": "John"
    }
  }
}
```

### 2. Date Format
- v1: `2025-01-15 10:30:00`
- v2: `2025-01-15T10:30:00Z` (ISO 8601)
```

### 2. 변환 도구 제공

```javascript
// v1-to-v2-adapter.js
function adaptV1toV2(v1Response) {
  return {
    data: {
      id: v1Response.id,
      attributes: {
        name: v1Response.name,
        email: v1Response.email
      }
    }
  };
}
```

### 3. 병렬 실행 지원

클라이언트가 점진적으로 마이그레이션할 수 있도록 양쪽 버전 동시 지원:

```javascript
// 두 버전 모두 지원
app.get('/v1/users', handleV1Users);
app.get('/v2/users', handleV2Users);

// 6개월 후 v1 제거
app.get('/v1/users', (req, res) => {
  res.status(410).json({
    error: 'This API version has been removed. Please use v2.'
  });
});
```

## 버전별 테스트

### 1. 버전별 테스트 분리

```javascript
// tests/v1/users.test.js
describe('Users API v1', () => {
  it('should return users in v1 format', async () => {
    const response = await request(app)
      .get('/v1/users')
      .expect(200);

    expect(response.body).toHaveProperty('id');
    expect(response.body).toHaveProperty('name');
  });
});

// tests/v2/users.test.js
describe('Users API v2', () => {
  it('should return users in v2 format', async () => {
    const response = await request(app)
      .get('/v2/users')
      .expect(200);

    expect(response.body).toHaveProperty('data');
    expect(response.body.data).toHaveProperty('id');
  });
});
```

### 2. 계약 테스트

```yaml
# pact/user-api-v2.yaml
interactions:
  - description: Get user by ID
    request:
      method: GET
      path: /v2/users/123
    response:
      status: 200
      body:
        data:
          id: "123"
          attributes:
            name: "John Doe"
```

## 버저닝 안티패턴

### 피해야 할 것들

1. **날짜 기반 버저닝**
   ```
   나쁜 예: /api/2025-01-15/users
   ```

2. **너무 세밀한 버저닝**
   ```
   나쁜 예: /api/v1.2.3/users
   ```

3. **일관성 없는 버저닝**
   ```
   나쁜 예:
   /v1/users
   /api/v2/products
   /version3/orders
   ```

4. **모든 변경에 버전 증가**
   - 버그 수정이나 내부 최적화는 버전 변경 불필요

## 권장 사항

1. **URI 버저닝 사용** (가장 실용적)
2. **Major 버전만 URI에 포함** (v1, v2, v3)
3. **최소 2개 버전 동시 지원**
4. **명확한 폐기 정책 수립**
5. **마이그레이션 가이드 제공**
6. **충분한 deprecation 기간 제공** (최소 6개월)
7. **버전별 문서 유지**
8. **자동화된 버전 테스트**

## 체크리스트

- [ ] 버저닝 전략 선택 (URI 권장)
- [ ] 버전 네임스페이스 구조 설계
- [ ] 공통 코드 재사용 계획
- [ ] 기본 버전 정책 수립
- [ ] 폐기 프로세스 정의
- [ ] 마이그레이션 가이드 작성
- [ ] 버전별 테스트 구현
- [ ] API 문서에 버전 정보 포함
- [ ] 모니터링 및 사용량 추적
- [ ] 클라이언트 알림 메커니즘 구축
