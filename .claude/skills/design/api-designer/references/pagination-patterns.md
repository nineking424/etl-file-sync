# 페이지네이션 패턴

대량의 데이터를 효율적으로 전송하기 위한 페이지네이션 전략과 구현 패턴을 설명합니다.

## 페이지네이션이 필요한 이유

1. **성능**: 한 번에 모든 데이터를 전송하면 느림
2. **메모리**: 클라이언트와 서버 메모리 절약
3. **대역폭**: 네트워크 트래픽 감소
4. **사용자 경험**: 점진적 로딩으로 빠른 초기 응답

## 주요 페이지네이션 전략

### 1. 오프셋 기반 페이지네이션

가장 일반적인 방식으로, 페이지 번호와 페이지 크기를 사용합니다.

#### 요청 예제

```
GET /users?page=2&limit=20
GET /products?offset=40&limit=20
```

#### 응답 예제

```json
{
  "data": [
    {"id": "21", "name": "User 21"},
    {"id": "22", "name": "User 22"},
    ...
  ],
  "pagination": {
    "page": 2,
    "limit": 20,
    "total": 100,
    "total_pages": 5
  },
  "links": {
    "first": "/users?page=1&limit=20",
    "prev": "/users?page=1&limit=20",
    "self": "/users?page=2&limit=20",
    "next": "/users?page=3&limit=20",
    "last": "/users?page=5&limit=20"
  }
}
```

#### 장점
- 구현이 간단
- 특정 페이지로 직접 이동 가능
- UI에서 페이지 번호 표시 용이

#### 단점
- 데이터가 추가/삭제되면 중복/누락 발생 가능
- 깊은 페이지로 갈수록 성능 저하
- 실시간 데이터에 부적합

#### SQL 구현

```sql
-- MySQL
SELECT * FROM users
ORDER BY created_at DESC
LIMIT 20 OFFSET 40;

-- PostgreSQL
SELECT * FROM users
ORDER BY created_at DESC
LIMIT 20 OFFSET 40;
```

#### 구현 예제 (Node.js)

```javascript
app.get('/users', async (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 20;
  const offset = (page - 1) * limit;

  // 데이터 조회
  const users = await db.query(
    'SELECT * FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?',
    [limit, offset]
  );

  // 전체 개수 조회
  const [{ total }] = await db.query('SELECT COUNT(*) as total FROM users');
  const totalPages = Math.ceil(total / limit);

  res.json({
    data: users,
    pagination: {
      page,
      limit,
      total,
      total_pages: totalPages
    },
    links: {
      first: `/users?page=1&limit=${limit}`,
      prev: page > 1 ? `/users?page=${page - 1}&limit=${limit}` : null,
      self: `/users?page=${page}&limit=${limit}`,
      next: page < totalPages ? `/users?page=${page + 1}&limit=${limit}` : null,
      last: `/users?page=${totalPages}&limit=${limit}`
    }
  });
});
```

### 2. 커서 기반 페이지네이션

고유 식별자를 커서로 사용하여 다음 데이터를 가져오는 방식입니다.

#### 요청 예제

```
GET /posts?cursor=eyJpZCI6MTIzfQ==&limit=20
GET /posts?after=post_123&limit=20
```

#### 응답 예제

```json
{
  "data": [
    {"id": "124", "title": "Post 124", "created_at": "2025-01-15T10:30:00Z"},
    {"id": "125", "title": "Post 125", "created_at": "2025-01-15T10:31:00Z"},
    ...
  ],
  "pagination": {
    "limit": 20,
    "next_cursor": "eyJpZCI6MTQzfQ==",
    "has_more": true
  },
  "links": {
    "self": "/posts?cursor=eyJpZCI6MTIzfQ==&limit=20",
    "next": "/posts?cursor=eyJpZCI6MTQzfQ==&limit=20"
  }
}
```

#### 장점
- 실시간 데이터에 적합
- 중복/누락 문제 없음
- 일관된 성능
- 무한 스크롤에 최적

#### 단점
- 특정 페이지로 직접 이동 불가
- 전체 개수 계산 어려움
- 구현이 복잡

#### SQL 구현

```sql
-- 첫 페이지
SELECT * FROM posts
ORDER BY created_at DESC, id DESC
LIMIT 20;

-- 다음 페이지 (cursor_id = 123)
SELECT * FROM posts
WHERE (created_at, id) < (
  SELECT created_at, id FROM posts WHERE id = 123
)
ORDER BY created_at DESC, id DESC
LIMIT 20;
```

#### 구현 예제 (Node.js)

```javascript
const crypto = require('crypto');

function encodeCursor(data) {
  return Buffer.from(JSON.stringify(data)).toString('base64');
}

function decodeCursor(cursor) {
  return JSON.parse(Buffer.from(cursor, 'base64').toString());
}

app.get('/posts', async (req, res) => {
  const limit = parseInt(req.query.limit) || 20;
  let query = 'SELECT * FROM posts';
  let params = [];

  if (req.query.cursor) {
    const { id, created_at } = decodeCursor(req.query.cursor);
    query += ' WHERE (created_at, id) < (?, ?)';
    params = [created_at, id];
  }

  query += ' ORDER BY created_at DESC, id DESC LIMIT ?';
  params.push(limit + 1); // 하나 더 가져와서 has_more 판단

  const posts = await db.query(query, params);
  const hasMore = posts.length > limit;
  const data = posts.slice(0, limit);

  let nextCursor = null;
  if (hasMore) {
    const lastPost = data[data.length - 1];
    nextCursor = encodeCursor({
      id: lastPost.id,
      created_at: lastPost.created_at
    });
  }

  res.json({
    data,
    pagination: {
      limit,
      next_cursor: nextCursor,
      has_more: hasMore
    },
    links: {
      self: req.query.cursor
        ? `/posts?cursor=${req.query.cursor}&limit=${limit}`
        : `/posts?limit=${limit}`,
      next: nextCursor ? `/posts?cursor=${nextCursor}&limit=${limit}` : null
    }
  });
});
```

### 3. Keyset 페이지네이션

커서 기반의 변형으로, 정렬 키를 명시적으로 사용합니다.

#### 요청 예제

```
GET /products?after_id=100&after_price=29.99&limit=20
```

#### 응답 예제

```json
{
  "data": [
    {"id": "101", "name": "Product 101", "price": 29.99},
    {"id": "102", "name": "Product 102", "price": 30.00},
    ...
  ],
  "pagination": {
    "limit": 20,
    "after_id": 120,
    "after_price": 35.50,
    "has_more": true
  }
}
```

#### SQL 구현

```sql
SELECT * FROM products
WHERE price > 29.99 OR (price = 29.99 AND id > 100)
ORDER BY price ASC, id ASC
LIMIT 20;
```

### 4. Seek Method (Range 기반)

시간 범위나 ID 범위를 사용하는 방식입니다.

#### 요청 예제

```
GET /events?start_id=1000&end_id=1100
GET /logs?start_time=2025-01-15T00:00:00Z&end_time=2025-01-15T23:59:59Z
```

#### 응답 예제

```json
{
  "data": [...],
  "range": {
    "start_id": 1000,
    "end_id": 1100,
    "returned_count": 100
  },
  "links": {
    "prev": "/events?start_id=900&end_id=1000",
    "next": "/events?start_id=1100&end_id=1200"
  }
}
```

## 페이지네이션 메타데이터

### 완전한 메타데이터

```json
{
  "data": [...],
  "pagination": {
    "current_page": 2,
    "per_page": 20,
    "total_items": 100,
    "total_pages": 5,
    "first_item": 21,
    "last_item": 40,
    "has_previous": true,
    "has_next": true
  }
}
```

### 최소 메타데이터 (커서 기반)

```json
{
  "data": [...],
  "pagination": {
    "limit": 20,
    "has_more": true,
    "next_cursor": "encoded_cursor_value"
  }
}
```

## HATEOAS 링크

### Link 헤더 사용

```http
HTTP/1.1 200 OK
Link: </users?page=1&limit=20>; rel="first",
      </users?page=1&limit=20>; rel="prev",
      </users?page=3&limit=20>; rel="next",
      </users?page=5&limit=20>; rel="last"
```

### 본문에 포함

```json
{
  "data": [...],
  "links": {
    "self": "/users?page=2&limit=20",
    "first": "/users?page=1&limit=20",
    "prev": "/users?page=1&limit=20",
    "next": "/users?page=3&limit=20",
    "last": "/users?page=5&limit=20"
  }
}
```

## 필터링과 결합

```
GET /products?category=electronics&price_min=100&page=2&limit=20
```

```json
{
  "data": [...],
  "filters": {
    "category": "electronics",
    "price_min": 100
  },
  "pagination": {
    "page": 2,
    "limit": 20,
    "total": 45,
    "total_pages": 3
  }
}
```

## 성능 최적화

### 1. COUNT 쿼리 최적화

```javascript
// 캐싱된 전체 개수 사용
const cachedTotal = await redis.get('users:total');
let total;

if (cachedTotal) {
  total = parseInt(cachedTotal);
} else {
  const [{ count }] = await db.query('SELECT COUNT(*) as count FROM users');
  total = count;
  await redis.set('users:total', total, 'EX', 300); // 5분 캐시
}
```

### 2. 인덱스 활용

```sql
-- 복합 인덱스 생성
CREATE INDEX idx_posts_created_id ON posts(created_at DESC, id DESC);

-- 효율적인 쿼리
SELECT * FROM posts
WHERE (created_at, id) < ('2025-01-15 10:30:00', 123)
ORDER BY created_at DESC, id DESC
LIMIT 20;
```

### 3. 대략적인 카운트

```sql
-- PostgreSQL 대략적 카운트
SELECT reltuples::bigint AS estimate
FROM pg_class
WHERE relname = 'users';
```

## GraphQL 페이지네이션

### Relay Cursor Connections

```graphql
query {
  users(first: 20, after: "cursor123") {
    edges {
      cursor
      node {
        id
        name
        email
      }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
    totalCount
  }
}
```

## 모바일 앱 고려사항

### 무한 스크롤

```json
{
  "data": [...],
  "pagination": {
    "next_cursor": "cursor_value",
    "has_more": true
  },
  "prefetch_url": "/posts?cursor=next_cursor_value&limit=20"
}
```

### 프리페칭

클라이언트가 다음 페이지를 미리 로드할 수 있도록 지원:

```javascript
app.get('/posts', async (req, res) => {
  // ... 현재 페이지 데이터 조회

  res.json({
    data,
    pagination: {
      next_cursor: nextCursor,
      has_more: hasMore
    },
    prefetch: {
      next_url: nextCursor ? `/posts?cursor=${nextCursor}&limit=${limit}` : null
    }
  });
});
```

## 에러 처리

### 잘못된 페이지 번호

```json
{
  "type": "https://api.example.com/errors/invalid-page",
  "title": "Invalid Page Number",
  "status": 400,
  "detail": "Page number must be between 1 and 5",
  "pagination": {
    "requested_page": 10,
    "max_page": 5
  }
}
```

### 잘못된 커서

```json
{
  "type": "https://api.example.com/errors/invalid-cursor",
  "title": "Invalid Cursor",
  "status": 400,
  "detail": "The provided cursor is invalid or expired"
}
```

## 베스트 프랙티스

### 1. 기본값 설정

```javascript
const page = Math.max(1, parseInt(req.query.page) || 1);
const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 20));
```

### 2. 최대 제한 설정

```javascript
const MAX_LIMIT = 100;
const limit = Math.min(MAX_LIMIT, parseInt(req.query.limit) || 20);
```

### 3. 일관된 정렬

항상 고유한 필드를 정렬에 포함:

```sql
-- 나쁜 예
ORDER BY created_at DESC

-- 좋은 예
ORDER BY created_at DESC, id DESC
```

## 선택 가이드

### 오프셋 기반 사용 시기
- 관리자 페이지, 대시보드
- 페이지 번호가 필요한 경우
- 데이터가 자주 변경되지 않는 경우
- 작은 데이터셋

### 커서 기반 사용 시기
- 무한 스크롤
- 실시간 피드
- 모바일 앱
- 대용량 데이터셋
- 데이터가 자주 변경되는 경우

## 체크리스트

- [ ] 페이지네이션 전략 선택
- [ ] 기본값 및 최대값 설정
- [ ] 일관된 정렬 순서
- [ ] 메타데이터 제공
- [ ] 링크 제공 (HATEOAS)
- [ ] 에러 처리
- [ ] 성능 최적화 (인덱스)
- [ ] COUNT 쿼리 최적화
- [ ] 필터링과 통합
- [ ] API 문서화
