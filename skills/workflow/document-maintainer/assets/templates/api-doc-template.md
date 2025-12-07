# API Documentation

## Overview

**Base URL:** `https://api.example.com/v1`

**Authentication:** Bearer token (JWT)

**Content Type:** `application/json`

**Rate Limiting:** 100 requests per minute per API key

## Authentication

### Obtain Access Token

```http
POST /auth/login
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "your-password"
}
```

**Response:**

```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expiresIn": 3600,
  "refreshToken": "refresh-token-here"
}
```

### Using the Token

Include the token in the Authorization header for all authenticated requests:

```http
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

## Endpoints

### Users

#### List Users

Retrieves a paginated list of users.

```http
GET /users
```

**Query Parameters:**

| Parameter | Type    | Required | Default | Description                    |
|-----------|---------|----------|---------|--------------------------------|
| page      | integer | No       | 1       | Page number                    |
| limit     | integer | No       | 10      | Items per page (max 100)       |
| search    | string  | No       | -       | Search by name or email        |
| role      | string  | No       | -       | Filter by role (admin, user)   |
| status    | string  | No       | -       | Filter by status (active, inactive) |
| sort      | string  | No       | createdAt | Sort field (name, email, createdAt) |
| order     | string  | No       | desc    | Sort order (asc, desc)         |

**Request Example:**

```bash
curl -X GET "https://api.example.com/v1/users?page=1&limit=10&role=admin" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response Example (200 OK):**

```json
{
  "data": [
    {
      "id": "123e4567-e89b-12d3-a456-426614174000",
      "email": "admin@example.com",
      "name": "Admin User",
      "role": "admin",
      "status": "active",
      "createdAt": "2025-01-15T10:00:00Z",
      "updatedAt": "2025-01-15T10:00:00Z"
    },
    {
      "id": "987e6543-e21b-34c5-a654-321456789000",
      "email": "user@example.com",
      "name": "Regular User",
      "role": "user",
      "status": "active",
      "createdAt": "2025-01-14T15:30:00Z",
      "updatedAt": "2025-01-14T15:30:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 10,
    "total": 42,
    "totalPages": 5
  }
}
```

**Error Responses:**

| Status Code | Description              | Response                                    |
|-------------|--------------------------|---------------------------------------------|
| 400         | Invalid query parameters | `{"error": "Invalid parameter", "message": "..."}` |
| 401         | Unauthorized             | `{"error": "Unauthorized", "message": "Invalid token"}` |
| 403         | Forbidden                | `{"error": "Forbidden", "message": "Insufficient permissions"}` |

---

#### Get User by ID

Retrieves detailed information about a specific user.

```http
GET /users/:id
```

**Path Parameters:**

| Parameter | Type   | Required | Description         |
|-----------|--------|----------|---------------------|
| id        | string | Yes      | User ID (UUID)      |

**Request Example:**

```bash
curl -X GET "https://api.example.com/v1/users/123e4567-e89b-12d3-a456-426614174000" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response Example (200 OK):**

```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "email": "user@example.com",
  "name": "John Doe",
  "role": "user",
  "status": "active",
  "profile": {
    "bio": "Software developer",
    "avatar": "https://cdn.example.com/avatars/123.jpg",
    "location": "San Francisco, CA"
  },
  "createdAt": "2025-01-15T10:00:00Z",
  "updatedAt": "2025-01-15T10:00:00Z",
  "lastLoginAt": "2025-01-20T14:30:00Z"
}
```

**Error Responses:**

| Status Code | Description    | Response                                          |
|-------------|----------------|---------------------------------------------------|
| 404         | User not found | `{"error": "Not Found", "message": "User not found"}` |
| 401         | Unauthorized   | `{"error": "Unauthorized", "message": "Invalid token"}` |

---

#### Create User

Creates a new user in the system.

```http
POST /users
```

**Request Body:**

| Field    | Type   | Required | Description                           |
|----------|--------|----------|---------------------------------------|
| email    | string | Yes      | User's email (must be unique)         |
| name     | string | Yes      | User's full name                      |
| password | string | Yes      | Password (min 8 characters)           |
| role     | string | No       | User role (default: "user")           |

**Request Example:**

```bash
curl -X POST "https://api.example.com/v1/users" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "newuser@example.com",
    "name": "New User",
    "password": "securePassword123",
    "role": "user"
  }'
```

**Response Example (201 Created):**

```json
{
  "id": "987e6543-e21b-34c5-a654-321456789000",
  "email": "newuser@example.com",
  "name": "New User",
  "role": "user",
  "status": "active",
  "createdAt": "2025-01-20T16:45:00Z",
  "updatedAt": "2025-01-20T16:45:00Z"
}
```

**Error Responses:**

| Status Code | Description            | Response Example                                     |
|-------------|------------------------|------------------------------------------------------|
| 400         | Validation error       | `{"error": "Validation failed", "details": [...]}` |
| 409         | Email already exists   | `{"error": "Conflict", "message": "Email already in use"}` |
| 401         | Unauthorized           | `{"error": "Unauthorized", "message": "Invalid token"}` |

**Validation Rules:**

- Email must be a valid email format
- Password must be at least 8 characters and contain:
  - At least one uppercase letter
  - At least one lowercase letter
  - At least one number
- Name must be between 1 and 100 characters
- Role must be one of: `admin`, `user`, `moderator`

---

#### Update User

Updates an existing user's information.

```http
PATCH /users/:id
```

**Path Parameters:**

| Parameter | Type   | Required | Description    |
|-----------|--------|----------|----------------|
| id        | string | Yes      | User ID (UUID) |

**Request Body:**

| Field  | Type   | Required | Description              |
|--------|--------|----------|--------------------------|
| email  | string | No       | New email address        |
| name   | string | No       | New name                 |
| role   | string | No       | New role                 |
| status | string | No       | New status               |

**Request Example:**

```bash
curl -X PATCH "https://api.example.com/v1/users/123e4567-e89b-12d3-a456-426614174000" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Updated Name",
    "status": "inactive"
  }'
```

**Response Example (200 OK):**

```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "email": "user@example.com",
  "name": "Updated Name",
  "role": "user",
  "status": "inactive",
  "createdAt": "2025-01-15T10:00:00Z",
  "updatedAt": "2025-01-20T17:00:00Z"
}
```

**Error Responses:**

| Status Code | Description      | Response                                          |
|-------------|------------------|---------------------------------------------------|
| 400         | Validation error | `{"error": "Validation failed", "details": [...]}` |
| 404         | User not found   | `{"error": "Not Found", "message": "User not found"}` |
| 403         | Forbidden        | `{"error": "Forbidden", "message": "Cannot modify this user"}` |

---

#### Delete User

Permanently deletes a user from the system.

```http
DELETE /users/:id
```

**Path Parameters:**

| Parameter | Type   | Required | Description    |
|-----------|--------|----------|----------------|
| id        | string | Yes      | User ID (UUID) |

**Request Example:**

```bash
curl -X DELETE "https://api.example.com/v1/users/123e4567-e89b-12d3-a456-426614174000" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response Example (204 No Content):**

```
(Empty response body)
```

**Error Responses:**

| Status Code | Description    | Response                                          |
|-------------|----------------|---------------------------------------------------|
| 404         | User not found | `{"error": "Not Found", "message": "User not found"}` |
| 403         | Forbidden      | `{"error": "Forbidden", "message": "Cannot delete this user"}` |
| 409         | Conflict       | `{"error": "Conflict", "message": "User has active resources"}` |

---

### Posts

#### Create Post

Creates a new post.

```http
POST /posts
```

**Request Body:**

| Field     | Type   | Required | Description                        |
|-----------|--------|----------|------------------------------------|
| title     | string | Yes      | Post title (max 200 characters)    |
| content   | string | Yes      | Post content                       |
| tags      | array  | No       | Array of tag strings               |
| published | boolean| No       | Publish immediately (default: false)|

**Request Example:**

```bash
curl -X POST "https://api.example.com/v1/posts" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "My First Post",
    "content": "This is the content of my first post.",
    "tags": ["tutorial", "beginners"],
    "published": true
  }'
```

**Response Example (201 Created):**

```json
{
  "id": "abc12345-def6-7890-ghij-klmnopqrstuv",
  "title": "My First Post",
  "content": "This is the content of my first post.",
  "tags": ["tutorial", "beginners"],
  "published": true,
  "authorId": "123e4567-e89b-12d3-a456-426614174000",
  "createdAt": "2025-01-20T18:00:00Z",
  "updatedAt": "2025-01-20T18:00:00Z"
}
```

---

## Error Handling

All error responses follow this format:

```json
{
  "error": "Error Type",
  "message": "Human-readable error message",
  "statusCode": 400,
  "requestId": "req-123456",
  "timestamp": "2025-01-20T18:00:00Z"
}
```

### Validation Errors

Validation errors include detailed information about each validation failure:

```json
{
  "error": "Validation Error",
  "message": "Request validation failed",
  "statusCode": 400,
  "details": [
    {
      "field": "email",
      "message": "Invalid email format",
      "value": "invalid-email"
    },
    {
      "field": "password",
      "message": "Password must be at least 8 characters",
      "value": "short"
    }
  ]
}
```

### Common Error Codes

| Status Code | Error Type              | Description                           |
|-------------|-------------------------|---------------------------------------|
| 400         | Bad Request             | Invalid request parameters or body    |
| 401         | Unauthorized            | Missing or invalid authentication     |
| 403         | Forbidden               | Insufficient permissions              |
| 404         | Not Found               | Resource not found                    |
| 409         | Conflict                | Resource conflict (e.g., duplicate)   |
| 422         | Unprocessable Entity    | Validation failed                     |
| 429         | Too Many Requests       | Rate limit exceeded                   |
| 500         | Internal Server Error   | Server error                          |
| 503         | Service Unavailable     | Service temporarily unavailable       |

## Rate Limiting

API requests are rate limited per API key:

- **Free tier:** 100 requests per minute
- **Pro tier:** 1000 requests per minute
- **Enterprise:** Custom limits

Rate limit information is included in response headers:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1642694400
```

When rate limit is exceeded, you'll receive a `429 Too Many Requests` response:

```json
{
  "error": "Rate Limit Exceeded",
  "message": "Too many requests. Please retry after 60 seconds.",
  "statusCode": 429,
  "retryAfter": 60
}
```

## Pagination

List endpoints support pagination using the following query parameters:

- `page`: Page number (default: 1)
- `limit`: Items per page (default: 10, max: 100)

Pagination information is included in the response:

```json
{
  "data": [...],
  "pagination": {
    "page": 1,
    "limit": 10,
    "total": 42,
    "totalPages": 5,
    "hasNext": true,
    "hasPrev": false
  }
}
```

## Filtering and Sorting

Most list endpoints support filtering and sorting:

### Filtering

Use query parameters to filter results:

```
GET /users?role=admin&status=active
```

### Sorting

Use `sort` and `order` parameters:

```
GET /users?sort=createdAt&order=desc
```

Multiple sort fields:

```
GET /users?sort=role,createdAt&order=asc,desc
```

## Versioning

The API is versioned using URL path versioning:

```
https://api.example.com/v1/users
https://api.example.com/v2/users
```

Current version: **v1**

## Webhooks

Configure webhooks to receive real-time notifications about events.

### Webhook Events

| Event             | Description                  |
|-------------------|------------------------------|
| user.created      | New user created             |
| user.updated      | User updated                 |
| user.deleted      | User deleted                 |
| post.created      | New post created             |
| post.published    | Post published               |

### Webhook Payload

```json
{
  "event": "user.created",
  "timestamp": "2025-01-20T18:00:00Z",
  "data": {
    "id": "123e4567-e89b-12d3-a456-426614174000",
    "email": "user@example.com",
    "name": "John Doe"
  }
}
```

### Webhook Security

Webhooks include a signature in the `X-Webhook-Signature` header. Verify the signature to ensure the request is from our servers.

## SDKs and Libraries

Official SDKs are available for:

- **JavaScript/Node.js:** `npm install @example/api-client`
- **Python:** `pip install example-api-client`
- **Java:** Available on Maven Central
- **Go:** `go get github.com/example/api-client-go`

### JavaScript Example

```javascript
const ExampleAPI = require('@example/api-client');

const client = new ExampleAPI({
  apiKey: 'your-api-key',
  environment: 'production'
});

const users = await client.users.list({
  page: 1,
  limit: 10
});
```

## Code Examples

### Complete CRUD Operations

```javascript
const axios = require('axios');

const API_URL = 'https://api.example.com/v1';
const API_TOKEN = 'your-token-here';

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Authorization': `Bearer ${API_TOKEN}`,
    'Content-Type': 'application/json'
  }
});

// Create user
const createUser = async () => {
  const response = await api.post('/users', {
    email: 'newuser@example.com',
    name: 'New User',
    password: 'securePassword123'
  });
  return response.data;
};

// Get user
const getUser = async (userId) => {
  const response = await api.get(`/users/${userId}`);
  return response.data;
};

// Update user
const updateUser = async (userId, updates) => {
  const response = await api.patch(`/users/${userId}`, updates);
  return response.data;
};

// Delete user
const deleteUser = async (userId) => {
  await api.delete(`/users/${userId}`);
};

// List users with pagination
const listUsers = async (page = 1, limit = 10) => {
  const response = await api.get('/users', {
    params: { page, limit }
  });
  return response.data;
};
```

## Testing

Use the following test credentials for the sandbox environment:

**Sandbox URL:** `https://sandbox-api.example.com/v1`

**Test API Key:** `test_sk_1234567890abcdef`

**Test Users:**
- Admin: `admin@test.example.com` / `testAdmin123`
- User: `user@test.example.com` / `testUser123`

## Support

- **API Status:** https://status.example.com
- **Developer Forum:** https://forum.example.com
- **Email Support:** api-support@example.com
- **Documentation:** https://docs.example.com

## Changelog

See [API Changelog](./CHANGELOG.md) for version history and breaking changes.