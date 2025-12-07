# Diagram Conventions

This document outlines conventions and best practices for creating diagrams using Mermaid syntax.

## Mermaid Diagram Types

### 1. Flowcharts

**Use for:** Process flows, decision trees, algorithms, workflows

#### Basic Syntax

```mermaid
flowchart TD
    A[Start] --> B{Is Valid?}
    B -->|Yes| C[Process Data]
    B -->|No| D[Show Error]
    C --> E[Save to Database]
    E --> F[Send Notification]
    F --> G[End]
    D --> G
```

#### Node Shapes

```mermaid
flowchart LR
    A[Rectangle - Process]
    B([Rounded - Start/End])
    C[(Database)]
    D((Circle))
    E{Diamond - Decision}
    F>Flag]
    G[/Parallelogram - Input/]
    H[\Parallelogram - Output\]
```

#### Direction Options

- `TD` or `TB`: Top to bottom
- `BT`: Bottom to top
- `LR`: Left to right
- `RL`: Right to left

#### Best Practices

- Keep flowcharts simple (max 10-15 nodes)
- Use consistent node shapes for similar concepts
- Label decision branches clearly (Yes/No, True/False)
- Align nodes visually when possible
- Use subgraphs for complex sections

#### Example: User Authentication Flow

```mermaid
flowchart TD
    Start([User Login]) --> Input[Enter Credentials]
    Input --> Validate{Valid Credentials?}
    Validate -->|Yes| CheckMFA{MFA Enabled?}
    Validate -->|No| Error[Show Error Message]
    CheckMFA -->|Yes| MFAPrompt[Prompt for MFA Code]
    CheckMFA -->|No| Success[Login Successful]
    MFAPrompt --> ValidateMFA{Valid MFA Code?}
    ValidateMFA -->|Yes| Success
    ValidateMFA -->|No| Error
    Error --> Input
    Success --> Dashboard[Redirect to Dashboard]
    Dashboard --> End([End])
```

### 2. Sequence Diagrams

**Use for:** API interactions, request/response flows, inter-service communication

#### Basic Syntax

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Database

    Client->>API: POST /users
    API->>API: Validate Input
    API->>Database: INSERT user
    Database-->>API: User Created
    API-->>Client: 201 Created
```

#### Features

- **Participants**: Define actors/systems
- **Messages**: Solid arrows for requests, dashed for responses
- **Activation**: Show when a participant is active
- **Notes**: Add explanatory text
- **Loops**: Show repeated actions
- **Alternatives**: Show conditional flows

#### Advanced Example

```mermaid
sequenceDiagram
    actor User
    participant Frontend
    participant API Gateway
    participant Auth Service
    participant User Service
    participant Database

    User->>Frontend: Click Login
    activate Frontend
    Frontend->>API Gateway: POST /auth/login
    activate API Gateway

    API Gateway->>Auth Service: Validate Credentials
    activate Auth Service
    Auth Service->>Database: Query User
    activate Database
    Database-->>Auth Service: User Data
    deactivate Database

    alt Credentials Valid
        Auth Service->>Auth Service: Generate JWT
        Auth Service-->>API Gateway: JWT Token
        API Gateway-->>Frontend: 200 OK + Token
        Frontend->>Frontend: Store Token
        Frontend-->>User: Show Dashboard
    else Credentials Invalid
        Auth Service-->>API Gateway: 401 Unauthorized
        API Gateway-->>Frontend: 401 Error
        Frontend-->>User: Show Error Message
    end

    deactivate Auth Service
    deactivate API Gateway
    deactivate Frontend
```

#### Best Practices

- List participants in order of interaction
- Use meaningful participant names
- Add notes for complex logic
- Show error paths with alt/opt
- Indicate async operations clearly

### 3. Class Diagrams

**Use for:** Object-oriented design, data models, entity relationships

#### Basic Syntax

```mermaid
classDiagram
    class User {
        +String id
        +String email
        +String name
        -String password
        +login()
        +logout()
        +updateProfile()
    }

    class Admin {
        +String permissions
        +manageUsers()
        +viewLogs()
    }

    class Post {
        +String id
        +String title
        +String content
        +Date createdAt
        +publish()
        +delete()
    }

    User <|-- Admin
    User "1" --> "*" Post : creates
```

#### Visibility Modifiers

- `+` Public
- `-` Private
- `#` Protected
- `~` Package/Internal

#### Relationships

- `<|--` Inheritance
- `*--` Composition
- `o--` Aggregation
- `-->` Association
- `..>` Dependency
- `..|>` Realization

#### Example: E-commerce Domain Model

```mermaid
classDiagram
    class User {
        +UUID id
        +String email
        +String name
        +Address[] addresses
        +register()
        +login()
    }

    class Order {
        +UUID id
        +Date createdAt
        +OrderStatus status
        +Decimal total
        +placeOrder()
        +cancel()
        +getTotal()
    }

    class OrderItem {
        +UUID id
        +int quantity
        +Decimal price
        +calculateSubtotal()
    }

    class Product {
        +UUID id
        +String name
        +String description
        +Decimal price
        +int stock
        +updateStock()
    }

    class Payment {
        +UUID id
        +PaymentMethod method
        +PaymentStatus status
        +Decimal amount
        +process()
        +refund()
    }

    User "1" --> "*" Order : places
    Order "1" *-- "*" OrderItem : contains
    OrderItem "*" --> "1" Product : references
    Order "1" --> "1" Payment : requires
```

### 4. Entity Relationship Diagrams

**Use for:** Database schemas, data modeling

#### Basic Syntax

```mermaid
erDiagram
    USER ||--o{ ORDER : places
    ORDER ||--|{ ORDER_ITEM : contains
    PRODUCT ||--o{ ORDER_ITEM : "ordered in"
    ORDER ||--|| PAYMENT : has

    USER {
        uuid id PK
        string email UK
        string name
        timestamp created_at
    }

    ORDER {
        uuid id PK
        uuid user_id FK
        decimal total
        string status
        timestamp created_at
    }

    ORDER_ITEM {
        uuid id PK
        uuid order_id FK
        uuid product_id FK
        int quantity
        decimal price
    }

    PRODUCT {
        uuid id PK
        string name
        string description
        decimal price
        int stock
    }

    PAYMENT {
        uuid id PK
        uuid order_id FK
        string method
        string status
        decimal amount
    }
```

#### Relationship Types

- `||--||` One to one
- `||--o{` One to many
- `}o--o{` Many to many
- `||--o|` One to zero or one

#### Cardinality

- `|o` Zero or one
- `||` Exactly one
- `}o` Zero or more
- `}|` One or more

### 5. State Diagrams

**Use for:** Application states, state machines, workflow states

#### Basic Syntax

```mermaid
stateDiagram-v2
    [*] --> Draft
    Draft --> Review: submit
    Review --> Approved: approve
    Review --> Rejected: reject
    Review --> Draft: request_changes
    Approved --> Published: publish
    Rejected --> Draft: revise
    Published --> Archived: archive
    Archived --> [*]
```

#### Example: Order Status Flow

```mermaid
stateDiagram-v2
    [*] --> Created
    Created --> Pending: place_order

    Pending --> Processing: confirm_payment
    Pending --> Cancelled: cancel

    Processing --> Shipped: ship
    Processing --> Failed: payment_failed

    Shipped --> Delivered: deliver
    Shipped --> Returned: return_request

    Delivered --> Completed: confirm_delivery
    Returned --> Refunded: process_refund

    Failed --> [*]
    Cancelled --> [*]
    Completed --> [*]
    Refunded --> [*]

    note right of Processing
        Payment is being processed
        and order is being prepared
    end note
```

### 6. Gantt Charts

**Use for:** Project timelines, sprints, task scheduling

#### Basic Syntax

```mermaid
gantt
    title Project Development Timeline
    dateFormat YYYY-MM-DD
    section Planning
    Requirements Gathering    :done, req, 2025-01-01, 2025-01-10
    Design Phase              :done, design, 2025-01-11, 2025-01-20

    section Development
    Backend API               :active, backend, 2025-01-21, 2025-02-15
    Frontend UI               :frontend, 2025-01-28, 2025-02-20
    Database Setup            :done, db, 2025-01-21, 2025-01-25

    section Testing
    Unit Testing              :testing, 2025-02-10, 2025-02-25
    Integration Testing       :int-test, 2025-02-20, 2025-03-05

    section Deployment
    Staging Deployment        :staging, 2025-03-01, 2025-03-03
    Production Deployment     :prod, 2025-03-06, 2025-03-08
```

### 7. Pie Charts

**Use for:** Proportions, distributions, percentages

```mermaid
pie title Technology Stack Usage
    "JavaScript" : 35
    "Python" : 25
    "Java" : 20
    "Go" : 15
    "Other" : 5
```

### 8. Git Graph

**Use for:** Branch strategies, git workflows

```mermaid
gitGraph
    commit id: "Initial commit"
    commit id: "Add user model"
    branch feature/auth
    checkout feature/auth
    commit id: "Add login endpoint"
    commit id: "Add JWT auth"
    checkout main
    commit id: "Fix bug in user model"
    checkout feature/auth
    merge main
    commit id: "Add logout endpoint"
    checkout main
    merge feature/auth tag: "v1.0.0"
    commit id: "Update documentation"
```

### 9. C4 Diagrams

**Use for:** System architecture, component relationships

```mermaid
C4Context
    title System Context Diagram for Online Store

    Person(customer, "Customer", "A customer of the online store")
    System(store, "Online Store", "Allows customers to browse and purchase products")
    System_Ext(payment, "Payment Gateway", "Processes payments")
    System_Ext(email, "Email System", "Sends emails to customers")

    Rel(customer, store, "Uses")
    Rel(store, payment, "Processes payments using")
    Rel(store, email, "Sends emails using")
```

## Styling Conventions

### Colors

Use consistent colors for different types of nodes:

```mermaid
flowchart LR
    A[User Action]:::userAction
    B[System Process]:::systemProcess
    C[Database]:::database
    D[External Service]:::external

    classDef userAction fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    classDef systemProcess fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef database fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    classDef external fill:#fff3e0,stroke:#e65100,stroke-width:2px
```

### Consistent Naming

- Use **PascalCase** for classes and entities
- Use **camelCase** for methods and attributes
- Use **UPPER_CASE** for constants and table names
- Use descriptive names, avoid abbreviations

## Best Practices

### 1. Keep Diagrams Focused

- One concept per diagram
- Maximum 10-15 elements
- Break complex diagrams into multiple views

### 2. Use Consistent Layout

- Top-to-bottom for hierarchical flows
- Left-to-right for sequential processes
- Group related elements

### 3. Label Everything

- Clear node labels
- Descriptive edge labels
- Add notes for complex logic

### 4. Maintain Diagrams

- Update diagrams with code changes
- Version control diagrams with code
- Add diagram update date

### 5. Choose the Right Diagram Type

| Diagram Type | Best For |
|--------------|----------|
| Flowchart | Algorithms, processes, decision flows |
| Sequence | API calls, message flows, interactions |
| Class | Object models, inheritance, relationships |
| ER | Database schemas, data models |
| State | State machines, workflows, status flows |
| Gantt | Timelines, schedules, project plans |
| Pie | Proportions, distributions |

## Common Patterns

### Pattern 1: Microservices Architecture

```mermaid
flowchart TB
    subgraph Client
        Web[Web App]
        Mobile[Mobile App]
    end

    subgraph API Gateway
        Gateway[API Gateway]
    end

    subgraph Services
        Auth[Auth Service]
        User[User Service]
        Order[Order Service]
        Product[Product Service]
    end

    subgraph Data
        AuthDB[(Auth DB)]
        UserDB[(User DB)]
        OrderDB[(Order DB)]
        ProductDB[(Product DB)]
    end

    Web --> Gateway
    Mobile --> Gateway
    Gateway --> Auth
    Gateway --> User
    Gateway --> Order
    Gateway --> Product
    Auth --> AuthDB
    User --> UserDB
    Order --> OrderDB
    Product --> ProductDB
```

### Pattern 2: Request/Response Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant A as Auth
    participant S as Service
    participant D as Database

    C->>G: Request + Token
    G->>A: Validate Token
    A-->>G: Token Valid
    G->>S: Process Request
    S->>D: Query Data
    D-->>S: Data
    S-->>G: Response
    G-->>C: Response
```

### Pattern 3: Event-Driven Architecture

```mermaid
flowchart LR
    subgraph Producers
        A[Order Service]
        B[Payment Service]
    end

    subgraph Event Bus
        Queue[Message Queue]
    end

    subgraph Consumers
        C[Notification Service]
        D[Analytics Service]
        E[Inventory Service]
    end

    A -->|order.created| Queue
    B -->|payment.completed| Queue
    Queue -->|subscribe| C
    Queue -->|subscribe| D
    Queue -->|subscribe| E
```

## Testing Diagrams

### Online Tools

- [Mermaid Live Editor](https://mermaid.live/)
- [Mermaid Chart](https://www.mermaidchart.com/)

### Local Testing

Many markdown editors support Mermaid:
- VSCode (with extensions)
- Obsidian
- Typora
- GitHub/GitLab markdown

### Validation

Before committing diagrams:
1. Test syntax in Mermaid Live Editor
2. Verify rendering in target platform
3. Check for broken references
4. Validate against conventions

## Documentation

Always include:
- Diagram title/caption
- Brief description
- Legend if using custom styles
- Last update date

Example:
```markdown
### System Architecture

The following diagram shows the high-level architecture of the system.

```mermaid
[diagram code]
```

**Last Updated:** 2025-01-15
**Legend:**
- Blue: User-facing components
- Purple: Backend services
- Green: Data stores
```

## Accessibility

- Use descriptive labels
- Avoid color-only differentiation
- Provide text alternatives
- Ensure sufficient contrast

## Version Control

- Store diagrams as code (Mermaid syntax)
- Keep diagrams close to related code
- Update diagrams in same commit as code changes
- Include diagram changes in code reviews