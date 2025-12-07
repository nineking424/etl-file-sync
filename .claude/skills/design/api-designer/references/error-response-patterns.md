# 에러 응답 패턴

API의 에러 응답은 일관되고 명확해야 하며, 클라이언트가 문제를 이해하고 해결하는 데 도움이 되어야 합니다.

## RFC 7807 Problem Details

RFC 7807은 HTTP API 에러 응답을 위한 표준 형식을 정의합니다.

### 기본 구조

```json
{
  "type": "https://example.com/errors/validation-error",
  "title": "Validation Error",
  "status": 400,
  "detail": "The request body contains invalid data",
  "instance": "/users/create"
}
```

### 필드 설명

- **type** (string): 에러 타입을 식별하는 URI
- **title** (string): 에러에 대한 간단한 설명
- **status** (integer): HTTP 상태 코드
- **detail** (string): 에러에 대한 상세 설명
- **instance** (string): 에러가 발생한 특정 요청 식별자

## 일반적인 에러 패턴

### 1. 검증 에러 (400 Bad Request)

```json
{
  "type": "https://api.example.com/errors/validation-error",
  "title": "Validation Failed",
  "status": 400,
  "detail": "One or more fields failed validation",
  "instance": "/users",
  "errors": [
    {
      "field": "email",
      "message": "Email format is invalid",
      "code": "INVALID_EMAIL",
      "value": "invalid-email"
    },
    {
      "field": "password",
      "message": "Password must be at least 8 characters",
      "code": "PASSWORD_TOO_SHORT",
      "value": null
    }
  ]
}
```

### 2. 인증 에러 (401 Unauthorized)

```json
{
  "type": "https://api.example.com/errors/unauthorized",
  "title": "Authentication Required",
  "status": 401,
  "detail": "Valid authentication credentials are required",
  "instance": "/protected-resource",
  "authentication_url": "https://example.com/login"
}
```

### 3. 권한 에러 (403 Forbidden)

```json
{
  "type": "https://api.example.com/errors/forbidden",
  "title": "Access Denied",
  "status": 403,
  "detail": "You do not have permission to access this resource",
  "instance": "/admin/users",
  "required_permission": "admin:users:write"
}
```

### 4. 리소스 없음 (404 Not Found)

```json
{
  "type": "https://api.example.com/errors/not-found",
  "title": "Resource Not Found",
  "status": 404,
  "detail": "The requested user does not exist",
  "instance": "/users/999",
  "resource_type": "User",
  "resource_id": "999"
}
```

### 5. 충돌 에러 (409 Conflict)

```json
{
  "type": "https://api.example.com/errors/conflict",
  "title": "Resource Conflict",
  "status": 409,
  "detail": "A user with this email already exists",
  "instance": "/users",
  "conflicting_field": "email",
  "conflicting_value": "john@example.com",
  "existing_resource": "/users/123"
}
```

### 6. Rate Limit 초과 (429 Too Many Requests)

```json
{
  "type": "https://api.example.com/errors/rate-limit",
  "title": "Rate Limit Exceeded",
  "status": 429,
  "detail": "Too many requests. Please try again later",
  "instance": "/api/search",
  "retry_after": 60,
  "limit": 100,
  "remaining": 0,
  "reset_at": "2025-01-15T10:35:00Z"
}
```

### 7. 서버 에러 (500 Internal Server Error)

```json
{
  "type": "https://api.example.com/errors/internal-error",
  "title": "Internal Server Error",
  "status": 500,
  "detail": "An unexpected error occurred while processing your request",
  "instance": "/users/123/orders",
  "trace_id": "a3c4e5f6-b2d4-4e6f-8a1b-c3d4e5f6a7b8",
  "timestamp": "2025-01-15T10:30:00Z"
}
```

### 8. 서비스 이용 불가 (503 Service Unavailable)

```json
{
  "type": "https://api.example.com/errors/service-unavailable",
  "title": "Service Temporarily Unavailable",
  "status": 503,
  "detail": "The service is currently undergoing maintenance",
  "instance": "/",
  "retry_after": 3600,
  "estimated_restore_time": "2025-01-15T12:00:00Z"
}
```

## 비즈니스 로직 에러

### 잔액 부족

```json
{
  "type": "https://api.example.com/errors/insufficient-balance",
  "title": "Insufficient Balance",
  "status": 400,
  "detail": "Your account balance is insufficient for this transaction",
  "instance": "/transactions",
  "current_balance": 50.00,
  "required_amount": 100.00,
  "currency": "USD"
}
```

### 재고 부족

```json
{
  "type": "https://api.example.com/errors/out-of-stock",
  "title": "Product Out of Stock",
  "status": 409,
  "detail": "The requested quantity is not available",
  "instance": "/orders",
  "product_id": "prod-123",
  "requested_quantity": 10,
  "available_quantity": 5
}
```

## 에러 코드 체계

### 계층적 에러 코드

```json
{
  "type": "https://api.example.com/errors/validation-error",
  "title": "Validation Error",
  "status": 400,
  "detail": "Request validation failed",
  "code": "VALIDATION_ERROR",
  "errors": [
    {
      "field": "email",
      "code": "VALIDATION_ERROR.INVALID_FORMAT",
      "message": "Email format is invalid"
    },
    {
      "field": "age",
      "code": "VALIDATION_ERROR.OUT_OF_RANGE",
      "message": "Age must be between 18 and 120"
    }
  ]
}
```

### 에러 코드 카탈로그

```
AUTH_001: Invalid credentials
AUTH_002: Token expired
AUTH_003: Token invalid
AUTH_004: Missing authentication

VALIDATION_001: Required field missing
VALIDATION_002: Invalid format
VALIDATION_003: Value out of range
VALIDATION_004: Invalid data type

BUSINESS_001: Insufficient balance
BUSINESS_002: Duplicate resource
BUSINESS_003: Resource locked
BUSINESS_004: Operation not allowed

SYSTEM_001: Database error
SYSTEM_002: External service error
SYSTEM_003: Configuration error
```

## 다국어 지원

### Accept-Language 기반

```http
GET /users/invalid
Accept-Language: ko-KR

Response:
{
  "type": "https://api.example.com/errors/not-found",
  "title": "리소스를 찾을 수 없음",
  "status": 404,
  "detail": "요청한 사용자가 존재하지 않습니다",
  "instance": "/users/invalid"
}
```

### 메시지 키 제공

```json
{
  "type": "https://api.example.com/errors/validation-error",
  "title": "Validation Error",
  "status": 400,
  "detail": "Request validation failed",
  "message_key": "errors.validation.failed",
  "errors": [
    {
      "field": "email",
      "message_key": "errors.validation.email.invalid",
      "message": "Email format is invalid",
      "params": {
        "field": "email"
      }
    }
  ]
}
```

## 개발자 친화적 에러

### 디버깅 정보 포함 (개발 환경)

```json
{
  "type": "https://api.example.com/errors/internal-error",
  "title": "Internal Server Error",
  "status": 500,
  "detail": "Database query failed",
  "instance": "/users/123",
  "trace_id": "a3c4e5f6-b2d4-4e6f-8a1b-c3d4e5f6a7b8",
  "debug_info": {
    "exception": "DatabaseConnectionError",
    "message": "Connection to database timed out",
    "stack_trace": [
      "at DatabaseClient.query (db.js:45)",
      "at UserRepository.findById (user-repo.js:23)",
      "at UserService.getUser (user-service.js:15)"
    ],
    "query": "SELECT * FROM users WHERE id = ?",
    "params": [123]
  }
}
```

### 도움말 링크 제공

```json
{
  "type": "https://api.example.com/errors/validation-error",
  "title": "Validation Error",
  "status": 400,
  "detail": "Invalid request format",
  "instance": "/users",
  "help_url": "https://docs.example.com/api/users#validation",
  "errors": [
    {
      "field": "email",
      "message": "Email format is invalid",
      "help_url": "https://docs.example.com/api/validation/email"
    }
  ]
}
```

## 에러 응답 구현 예제

### Express.js (Node.js)

```javascript
class ApiError extends Error {
  constructor(type, title, status, detail, instance, additionalData = {}) {
    super(detail);
    this.type = type;
    this.title = title;
    this.status = status;
    this.detail = detail;
    this.instance = instance;
    Object.assign(this, additionalData);
  }

  toJSON() {
    return {
      type: this.type,
      title: this.title,
      status: this.status,
      detail: this.detail,
      instance: this.instance,
      ...Object.keys(this)
        .filter(key => !['type', 'title', 'status', 'detail', 'instance', 'name', 'message', 'stack'].includes(key))
        .reduce((acc, key) => ({ ...acc, [key]: this[key] }), {})
    };
  }
}

// 에러 핸들러 미들웨어
app.use((err, req, res, next) => {
  if (err instanceof ApiError) {
    return res.status(err.status).json(err.toJSON());
  }

  // 예상하지 못한 에러
  const internalError = new ApiError(
    'https://api.example.com/errors/internal-error',
    'Internal Server Error',
    500,
    'An unexpected error occurred',
    req.originalUrl,
    {
      trace_id: req.id,
      timestamp: new Date().toISOString()
    }
  );

  console.error(err);
  res.status(500).json(internalError.toJSON());
});

// 사용 예제
app.post('/users', async (req, res, next) => {
  try {
    const { email, name } = req.body;

    if (!email || !name) {
      throw new ApiError(
        'https://api.example.com/errors/validation-error',
        'Validation Error',
        400,
        'Required fields are missing',
        req.originalUrl,
        {
          errors: [
            !email && { field: 'email', message: 'Email is required', code: 'REQUIRED' },
            !name && { field: 'name', message: 'Name is required', code: 'REQUIRED' }
          ].filter(Boolean)
        }
      );
    }

    const user = await createUser({ email, name });
    res.status(201).json(user);
  } catch (err) {
    next(err);
  }
});
```

### Spring Boot (Java)

```java
@Data
@AllArgsConstructor
public class ApiError {
    private String type;
    private String title;
    private int status;
    private String detail;
    private String instance;
    private Map<String, Object> additionalData;
}

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ApiError> handleValidationException(
            ValidationException ex,
            HttpServletRequest request
    ) {
        List<FieldError> fieldErrors = ex.getFieldErrors().stream()
            .map(error -> new FieldError(
                error.getField(),
                error.getMessage(),
                error.getCode()
            ))
            .collect(Collectors.toList());

        Map<String, Object> additionalData = new HashMap<>();
        additionalData.put("errors", fieldErrors);

        ApiError error = new ApiError(
            "https://api.example.com/errors/validation-error",
            "Validation Error",
            400,
            "One or more fields failed validation",
            request.getRequestURI(),
            additionalData
        );

        return ResponseEntity.status(400).body(error);
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ApiError> handleNotFound(
            ResourceNotFoundException ex,
            HttpServletRequest request
    ) {
        Map<String, Object> additionalData = new HashMap<>();
        additionalData.put("resource_type", ex.getResourceType());
        additionalData.put("resource_id", ex.getResourceId());

        ApiError error = new ApiError(
            "https://api.example.com/errors/not-found",
            "Resource Not Found",
            404,
            ex.getMessage(),
            request.getRequestURI(),
            additionalData
        );

        return ResponseEntity.status(404).body(error);
    }
}
```

### FastAPI (Python)

```python
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class ErrorDetail(BaseModel):
    field: str
    message: str
    code: str

class ApiError(BaseModel):
    type: str
    title: str
    status: int
    detail: str
    instance: str
    errors: Optional[List[ErrorDetail]] = None
    additional_data: Optional[Dict[str, Any]] = None

app = FastAPI()

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    error = ApiError(
        type=f"https://api.example.com/errors/{exc.status_code}",
        title=exc.detail,
        status=exc.status_code,
        detail=exc.detail,
        instance=str(request.url.path)
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=error.dict(exclude_none=True)
    )

class ValidationError(Exception):
    def __init__(self, errors: List[ErrorDetail]):
        self.errors = errors

@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    error = ApiError(
        type="https://api.example.com/errors/validation-error",
        title="Validation Error",
        status=400,
        detail="One or more fields failed validation",
        instance=str(request.url.path),
        errors=exc.errors
    )
    return JSONResponse(
        status_code=400,
        content=error.dict(exclude_none=True)
    )
```

## 에러 로깅

### 구조화된 로깅

```javascript
logger.error('API Error', {
  error_type: error.type,
  error_title: error.title,
  status_code: error.status,
  instance: error.instance,
  trace_id: req.id,
  user_id: req.user?.id,
  ip_address: req.ip,
  user_agent: req.get('user-agent'),
  timestamp: new Date().toISOString()
});
```

## 체크리스트

- [ ] RFC 7807 형식 준수
- [ ] 일관된 에러 구조
- [ ] 명확한 에러 메시지
- [ ] 적절한 HTTP 상태 코드
- [ ] 에러 코드 체계 구축
- [ ] 디버깅 정보 포함 (개발 환경)
- [ ] 다국어 지원 고려
- [ ] 도움말 링크 제공
- [ ] 구조화된 로깅
- [ ] 에러 모니터링 설정
