# API Documentation Patterns

This document provides patterns and best practices for documenting RESTful APIs using OpenAPI/Swagger specifications.

## OpenAPI Specification Basics

### OpenAPI 3.0+ Structure

```yaml
openapi: 3.0.3
info:
  title: User Management API
  description: API for managing users in the system
  version: 1.0.0
  contact:
    name: API Support
    email: support@example.com
  license:
    name: MIT
    url: https://opensource.org/licenses/MIT

servers:
  - url: https://api.example.com/v1
    description: Production server
  - url: https://staging-api.example.com/v1
    description: Staging server
  - url: http://localhost:3000/v1
    description: Development server

paths:
  /users:
    get:
      summary: List all users
      description: Retrieves a paginated list of all users
      operationId: listUsers
      tags:
        - Users
      parameters:
        - name: page
          in: query
          description: Page number
          required: false
          schema:
            type: integer
            default: 1
            minimum: 1
        - name: limit
          in: query
          description: Number of items per page
          required: false
          schema:
            type: integer
            default: 10
            minimum: 1
            maximum: 100
      responses:
        '200':
          description: Successful response
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UserList'
        '400':
          description: Invalid request parameters
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
      security:
        - bearerAuth: []

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
          example: "123e4567-e89b-12d3-a456-426614174000"
        email:
          type: string
          format: email
          example: "user@example.com"
        name:
          type: string
          example: "Jane Doe"
        createdAt:
          type: string
          format: date-time
          example: "2025-01-15T10:30:00Z"

  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT
```

## Endpoint Documentation Patterns

### Pattern 1: CRUD Operations

#### Create Resource (POST)

```yaml
/users:
  post:
    summary: Create a new user
    description: |
      Creates a new user in the system. The email address must be unique.

      **Business Rules:**
      - Email must be unique
      - Password must be at least 8 characters
      - Name is required
    operationId: createUser
    tags:
      - Users
    requestBody:
      required: true
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/CreateUserRequest'
          examples:
            basic:
              summary: Basic user creation
              value:
                name: "Jane Doe"
                email: "jane@example.com"
                password: "securePassword123"
            withRole:
              summary: User with specific role
              value:
                name: "Admin User"
                email: "admin@example.com"
                password: "adminPassword123"
                role: "admin"
    responses:
      '201':
        description: User created successfully
        headers:
          Location:
            description: URL of the created user
            schema:
              type: string
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/User'
            examples:
              success:
                value:
                  id: "123e4567-e89b-12d3-a456-426614174000"
                  name: "Jane Doe"
                  email: "jane@example.com"
                  createdAt: "2025-01-15T10:30:00Z"
      '400':
        description: Invalid input
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ValidationError'
            examples:
              invalidEmail:
                value:
                  error: "Validation failed"
                  details:
                    - field: "email"
                      message: "Invalid email format"
      '409':
        description: Email already exists
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
            example:
              error: "Conflict"
              message: "User with this email already exists"
    security:
      - bearerAuth: []
```

#### Read Resource (GET)

```yaml
/users/{userId}:
  get:
    summary: Get user by ID
    description: Retrieves detailed information about a specific user
    operationId: getUserById
    tags:
      - Users
    parameters:
      - name: userId
        in: path
        required: true
        description: The unique identifier of the user
        schema:
          type: string
          format: uuid
        example: "123e4567-e89b-12d3-a456-426614174000"
    responses:
      '200':
        description: User found
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
            example:
              error: "Not Found"
              message: "User with ID 123 not found"
    security:
      - bearerAuth: []
```

#### Update Resource (PUT/PATCH)

```yaml
/users/{userId}:
  patch:
    summary: Update user
    description: |
      Updates specific fields of a user. Only provided fields will be updated.

      **Note:** Email changes require email verification.
    operationId: updateUser
    tags:
      - Users
    parameters:
      - name: userId
        in: path
        required: true
        schema:
          type: string
          format: uuid
    requestBody:
      required: true
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/UpdateUserRequest'
          examples:
            updateName:
              summary: Update user name only
              value:
                name: "Jane Smith"
            updateEmail:
              summary: Update email
              value:
                email: "newemail@example.com"
    responses:
      '200':
        description: User updated successfully
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/User'
      '400':
        description: Invalid input
      '404':
        description: User not found
    security:
      - bearerAuth: []
```

#### Delete Resource (DELETE)

```yaml
/users/{userId}:
  delete:
    summary: Delete user
    description: |
      Permanently deletes a user from the system.

      **Warning:** This action cannot be undone.
    operationId: deleteUser
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
      '204':
        description: User deleted successfully
      '404':
        description: User not found
      '403':
        description: Insufficient permissions
    security:
      - bearerAuth: []
```

### Pattern 2: Search and Filtering

```yaml
/users/search:
  get:
    summary: Search users
    description: Search users with various filters
    operationId: searchUsers
    tags:
      - Users
    parameters:
      - name: q
        in: query
        description: Search query (searches name and email)
        schema:
          type: string
        example: "john"
      - name: role
        in: query
        description: Filter by role
        schema:
          type: string
          enum: [admin, user, moderator]
      - name: status
        in: query
        description: Filter by account status
        schema:
          type: string
          enum: [active, inactive, suspended]
      - name: createdAfter
        in: query
        description: Filter users created after this date
        schema:
          type: string
          format: date-time
      - name: sort
        in: query
        description: Sort field
        schema:
          type: string
          enum: [name, email, createdAt]
          default: createdAt
      - name: order
        in: query
        description: Sort order
        schema:
          type: string
          enum: [asc, desc]
          default: desc
    responses:
      '200':
        description: Search results
        content:
          application/json:
            schema:
              type: object
              properties:
                data:
                  type: array
                  items:
                    $ref: '#/components/schemas/User'
                pagination:
                  $ref: '#/components/schemas/Pagination'
                filters:
                  type: object
                  description: Applied filters
```

### Pattern 3: Batch Operations

```yaml
/users/batch:
  post:
    summary: Create multiple users
    description: Creates multiple users in a single request
    operationId: batchCreateUsers
    tags:
      - Users
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              users:
                type: array
                items:
                  $ref: '#/components/schemas/CreateUserRequest'
                minItems: 1
                maxItems: 100
    responses:
      '207':
        description: Multi-status response
        content:
          application/json:
            schema:
              type: object
              properties:
                results:
                  type: array
                  items:
                    type: object
                    properties:
                      status:
                        type: integer
                        example: 201
                      data:
                        $ref: '#/components/schemas/User'
                      error:
                        type: string
```

### Pattern 4: File Upload

```yaml
/users/{userId}/avatar:
  post:
    summary: Upload user avatar
    description: Uploads a profile picture for the user
    operationId: uploadAvatar
    tags:
      - Users
    parameters:
      - name: userId
        in: path
        required: true
        schema:
          type: string
    requestBody:
      required: true
      content:
        multipart/form-data:
          schema:
            type: object
            properties:
              file:
                type: string
                format: binary
                description: The image file (JPG, PNG, max 5MB)
            required:
              - file
    responses:
      '200':
        description: Avatar uploaded successfully
        content:
          application/json:
            schema:
              type: object
              properties:
                avatarUrl:
                  type: string
                  format: uri
                  example: "https://cdn.example.com/avatars/123.jpg"
      '400':
        description: Invalid file format or size
      '413':
        description: File too large
```

## Schema Patterns

### Pattern 1: Reusable Components

```yaml
components:
  schemas:
    # Base schemas
    Timestamp:
      type: object
      properties:
        createdAt:
          type: string
          format: date-time
        updatedAt:
          type: string
          format: date-time

    # Entity schemas
    User:
      allOf:
        - $ref: '#/components/schemas/Timestamp'
        - type: object
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
            role:
              type: string
              enum: [admin, user, moderator]
            status:
              type: string
              enum: [active, inactive, suspended]

    # Request schemas
    CreateUserRequest:
      type: object
      required:
        - email
        - name
        - password
      properties:
        email:
          type: string
          format: email
        name:
          type: string
          minLength: 1
          maxLength: 100
        password:
          type: string
          minLength: 8
          pattern: '^(?=.*[a-z])(?=.*[A-Z])(?=.*\d).+$'
          description: Must contain uppercase, lowercase, and number
        role:
          type: string
          enum: [admin, user, moderator]
          default: user

    UpdateUserRequest:
      type: object
      properties:
        email:
          type: string
          format: email
        name:
          type: string
          minLength: 1
          maxLength: 100

    # Response schemas
    UserList:
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

    # Error schemas
    Error:
      type: object
      required:
        - error
        - message
      properties:
        error:
          type: string
        message:
          type: string
        requestId:
          type: string
          format: uuid

    ValidationError:
      allOf:
        - $ref: '#/components/schemas/Error'
        - type: object
          properties:
            details:
              type: array
              items:
                type: object
                properties:
                  field:
                    type: string
                  message:
                    type: string
```

### Pattern 2: Polymorphic Schemas

```yaml
components:
  schemas:
    Event:
      type: object
      required:
        - eventType
      properties:
        eventType:
          type: string
      discriminator:
        propertyName: eventType
        mapping:
          user.created: '#/components/schemas/UserCreatedEvent'
          user.updated: '#/components/schemas/UserUpdatedEvent'

    UserCreatedEvent:
      allOf:
        - $ref: '#/components/schemas/Event'
        - type: object
          properties:
            userId:
              type: string
            userName:
              type: string

    UserUpdatedEvent:
      allOf:
        - $ref: '#/components/schemas/Event'
        - type: object
          properties:
            userId:
              type: string
            changes:
              type: object
```

## Authentication Patterns

### Pattern 1: JWT Bearer Token

```yaml
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT
      description: |
        JWT token obtained from /auth/login endpoint.

        Example: Authorization: Bearer eyJhbGc...

security:
  - bearerAuth: []

paths:
  /auth/login:
    post:
      summary: Login
      description: Authenticate user and receive JWT token
      tags:
        - Authentication
      security: []  # No auth required for login
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - email
                - password
              properties:
                email:
                  type: string
                  format: email
                password:
                  type: string
      responses:
        '200':
          description: Login successful
          content:
            application/json:
              schema:
                type: object
                properties:
                  token:
                    type: string
                    example: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
                  expiresIn:
                    type: integer
                    description: Token expiration time in seconds
                    example: 3600
```

### Pattern 2: API Key

```yaml
components:
  securitySchemes:
    apiKey:
      type: apiKey
      in: header
      name: X-API-Key
      description: API key for authentication
```

### Pattern 3: OAuth 2.0

```yaml
components:
  securitySchemes:
    oauth2:
      type: oauth2
      flows:
        authorizationCode:
          authorizationUrl: https://auth.example.com/oauth/authorize
          tokenUrl: https://auth.example.com/oauth/token
          scopes:
            read:users: Read user information
            write:users: Modify user information
            admin: Administrative access
```

## Error Response Patterns

### Standard Error Format

```yaml
components:
  schemas:
    ErrorResponse:
      type: object
      required:
        - error
        - message
        - statusCode
      properties:
        error:
          type: string
          description: Error type
          example: "ValidationError"
        message:
          type: string
          description: Human-readable error message
          example: "Invalid email format"
        statusCode:
          type: integer
          description: HTTP status code
          example: 400
        requestId:
          type: string
          format: uuid
          description: Unique request identifier for debugging
        timestamp:
          type: string
          format: date-time
        path:
          type: string
          description: API path that caused the error
        details:
          type: array
          items:
            type: object
            properties:
              field:
                type: string
              code:
                type: string
              message:
                type: string
```

### Common HTTP Status Codes

```yaml
responses:
  '200':
    description: Success
  '201':
    description: Created
  '204':
    description: No Content
  '400':
    description: Bad Request - Invalid input
  '401':
    description: Unauthorized - Missing or invalid authentication
  '403':
    description: Forbidden - Insufficient permissions
  '404':
    description: Not Found - Resource does not exist
  '409':
    description: Conflict - Resource already exists
  '422':
    description: Unprocessable Entity - Validation failed
  '429':
    description: Too Many Requests - Rate limit exceeded
  '500':
    description: Internal Server Error
  '503':
    description: Service Unavailable
```

## Versioning Patterns

### URL Path Versioning

```yaml
servers:
  - url: https://api.example.com/v1
    description: Version 1
  - url: https://api.example.com/v2
    description: Version 2 (latest)
```

### Header Versioning

```yaml
paths:
  /users:
    get:
      parameters:
        - name: API-Version
          in: header
          schema:
            type: string
            enum: [v1, v2]
            default: v2
```

## Best Practices

1. **Use clear, descriptive operation IDs**: Makes code generation easier
2. **Provide examples for all schemas**: Helps developers understand expected format
3. **Document all possible error responses**: Include status codes and error formats
4. **Use tags to group endpoints**: Organize API documentation logically
5. **Include security requirements**: Document authentication clearly
6. **Version your API**: Plan for evolution
7. **Validate your OpenAPI spec**: Use tools like Swagger Editor
8. **Keep descriptions concise**: Use additional documentation for details
9. **Use $ref for reusability**: DRY principle applies to API specs
10. **Include rate limiting info**: Document throttling policies

## Tools for OpenAPI

- **Swagger Editor**: Online editor for OpenAPI specs
- **Swagger UI**: Interactive API documentation
- **Redoc**: Clean, customizable API documentation
- **Postman**: Import OpenAPI specs for testing
- **OpenAPI Generator**: Generate client/server code
- **Spectral**: Linting tool for OpenAPI specs