# Secure Coding Practices

Language-specific secure coding guidelines and best practices.

## Python Secure Coding

### Input Validation

```python
# Bad: No validation
def process_age(age):
    return int(age) * 2

# Good: Comprehensive validation
def process_age(age):
    try:
        age_int = int(age)
        if not 0 <= age_int <= 150:
            raise ValueError("Age must be between 0 and 150")
        return age_int * 2
    except ValueError as e:
        logging.error(f"Invalid age input: {e}")
        raise

# Good: Using type hints and validation library
from pydantic import BaseModel, validator

class Person(BaseModel):
    age: int

    @validator('age')
    def validate_age(cls, v):
        if not 0 <= v <= 150:
            raise ValueError('Age must be between 0 and 150')
        return v
```

### SQL Injection Prevention

```python
# Bad: String formatting
def get_user_by_email(email):
    query = f"SELECT * FROM users WHERE email = '{email}'"
    return db.execute(query)

# Bad: % formatting
query = "SELECT * FROM users WHERE email = '%s'" % email

# Good: Parameterized query
def get_user_by_email(email):
    query = "SELECT * FROM users WHERE email = ?"
    return db.execute(query, (email,))

# Better: ORM
from sqlalchemy import select
def get_user_by_email(email):
    return session.execute(
        select(User).where(User.email == email)
    ).scalar_one_or_none()
```

### Command Injection Prevention

```python
# Bad: Using shell=True
import subprocess
filename = user_input
subprocess.run(f"cat {filename}", shell=True)

# Bad: os.system
os.system(f"rm {filename}")

# Good: Array of arguments, no shell
subprocess.run(["cat", filename], check=True, shell=False)

# Better: Use Python built-ins
with open(filename, 'r') as f:
    content = f.read()
```

### Path Traversal Prevention

```python
# Bad: Direct path construction
def read_file(filename):
    path = f"/var/www/uploads/{filename}"
    with open(path, 'r') as f:
        return f.read()

# Good: Path validation
import os
from pathlib import Path

def read_file(filename):
    base_dir = Path("/var/www/uploads")
    file_path = (base_dir / filename).resolve()

    # Ensure file is within base directory
    if not str(file_path).startswith(str(base_dir)):
        raise ValueError("Invalid file path")

    with open(file_path, 'r') as f:
        return f.read()
```

### Secure Random Generation

```python
# Bad: Predictable random
import random
token = random.randint(1000, 9999)  # NOT cryptographically secure

# Good: Cryptographically secure random
import secrets
token = secrets.token_urlsafe(32)
session_id = secrets.token_hex(16)
pin = secrets.randbelow(10000)  # Random number 0-9999
```

### Safe Deserialization

```python
# Bad: Pickle can execute arbitrary code
import pickle
data = pickle.loads(untrusted_data)  # DANGEROUS

# Good: Use JSON (safe for data only)
import json
data = json.loads(untrusted_data)

# Good: For complex objects, use safe serialization
from dataclasses import dataclass, asdict
import json

@dataclass
class User:
    id: int
    name: str

# Serialize
user_json = json.dumps(asdict(user))

# Deserialize
user_dict = json.loads(user_json)
user = User(**user_dict)
```

---

## JavaScript/Node.js Secure Coding

### XSS Prevention

```javascript
// Bad: Direct HTML insertion
const userInput = req.query.name;
document.getElementById('output').innerHTML = userInput;

// Bad: Using eval
eval(userProvidedCode);  // NEVER do this

// Good: Use textContent
document.getElementById('output').textContent = userInput;

// Good: Use DOM API
const div = document.createElement('div');
div.textContent = userInput;
document.getElementById('output').appendChild(div);

// Good: Use DOMPurify for HTML sanitization
import DOMPurify from 'dompurify';
const clean = DOMPurify.sanitize(userInput);
element.innerHTML = clean;
```

### SQL Injection Prevention

```javascript
// Bad: String concatenation
const query = `SELECT * FROM users WHERE email = '${email}'`;
db.query(query);

// Bad: Template literals in query
const query = `SELECT * FROM users WHERE id = ${userId}`;

// Good: Parameterized queries (mysql2)
const query = 'SELECT * FROM users WHERE email = ?';
db.query(query, [email]);

// Good: Named parameters (postgres)
const query = 'SELECT * FROM users WHERE email = $1';
db.query(query, [email]);

// Better: ORM (Sequelize)
const user = await User.findOne({ where: { email } });
```

### NoSQL Injection Prevention

```javascript
// Bad: Direct object insertion
const user = await db.collection('users').findOne({
    username: req.body.username,
    password: req.body.password
});

// If attacker sends: { username: "admin", password: { $ne: null } }
// This bypasses authentication!

// Good: Validate input types
const { username, password } = req.body;
if (typeof username !== 'string' || typeof password !== 'string') {
    throw new Error('Invalid input');
}

const user = await db.collection('users').findOne({
    username: username,
    password: hashPassword(password)
});

// Better: Use schema validation (MongoDB)
const userSchema = {
    validator: {
        $jsonSchema: {
            required: ['username', 'password'],
            properties: {
                username: { bsonType: 'string' },
                password: { bsonType: 'string' }
            }
        }
    }
};
```

### Prototype Pollution Prevention

```javascript
// Bad: Direct property assignment
function merge(target, source) {
    for (let key in source) {
        target[key] = source[key];
    }
}

// Attacker can send: {"__proto__": {"isAdmin": true}}

// Good: Check for prototype pollution
function merge(target, source) {
    for (let key in source) {
        if (key === '__proto__' || key === 'constructor' || key === 'prototype') {
            continue;
        }
        if (source.hasOwnProperty(key)) {
            target[key] = source[key];
        }
    }
}

// Better: Use safe alternatives
const merged = Object.assign({}, target, source);
// Or
const merged = { ...target, ...source };
```

### Secure Cookie Handling

```javascript
// Bad: Insecure cookie
res.cookie('session', sessionId);

// Good: Secure cookie configuration
res.cookie('session', sessionId, {
    httpOnly: true,      // Prevents XSS access
    secure: true,        // HTTPS only
    sameSite: 'strict',  // CSRF protection
    maxAge: 3600000,     // 1 hour expiry
    signed: true         // Cookie signing
});
```

### JWT Security

```javascript
// Bad: Weak secret, no expiration
const token = jwt.sign({ userId: user.id }, 'secret');

// Good: Strong secret, expiration, algorithm specified
const token = jwt.sign(
    { userId: user.id },
    process.env.JWT_SECRET,  // Strong secret from env
    {
        algorithm: 'HS256',
        expiresIn: '1h',
        issuer: 'myapp'
    }
);

// Good: Verify with strict options
jwt.verify(token, process.env.JWT_SECRET, {
    algorithms: ['HS256'],  // Prevent algorithm confusion
    issuer: 'myapp'
});
```

### RegEx Denial of Service (ReDoS) Prevention

```javascript
// Bad: Vulnerable to ReDoS
const emailRegex = /^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+$/;
// Input: "aaaaaaaaaaaaaaaaaaaaaaaaa!" causes catastrophic backtracking

// Good: Simplified, efficient regex
const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

// Better: Use validator library
const validator = require('validator');
if (!validator.isEmail(email)) {
    throw new Error('Invalid email');
}

// Good: Set timeout for regex operations
const safeRegexTest = (regex, str, timeoutMs = 100) => {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Regex timeout')), timeoutMs);
        try {
            const result = regex.test(str);
            clearTimeout(timeout);
            resolve(result);
        } catch (e) {
            clearTimeout(timeout);
            reject(e);
        }
    });
};
```

---

## Java Secure Coding

### SQL Injection Prevention

```java
// Bad: String concatenation
String query = "SELECT * FROM users WHERE email = '" + email + "'";
Statement stmt = connection.createStatement();
ResultSet rs = stmt.executeQuery(query);

// Good: Prepared statements
String query = "SELECT * FROM users WHERE email = ?";
PreparedStatement pstmt = connection.prepareStatement(query);
pstmt.setString(1, email);
ResultSet rs = pstmt.executeQuery();

// Better: JPA/Hibernate with named parameters
TypedQuery<User> query = em.createQuery(
    "SELECT u FROM User u WHERE u.email = :email", User.class);
query.setParameter("email", email);
User user = query.getSingleResult();
```

### XML External Entity (XXE) Prevention

```java
// Bad: Default XML parser (vulnerable to XXE)
DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
DocumentBuilder db = dbf.newDocumentBuilder();
Document doc = db.parse(new InputSource(new StringReader(xmlString)));

// Good: Disable external entities
DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
dbf.setXIncludeAware(false);
dbf.setExpandEntityReferences(false);

DocumentBuilder db = dbf.newDocumentBuilder();
Document doc = db.parse(new InputSource(new StringReader(xmlString)));
```

### Insecure Deserialization Prevention

```java
// Bad: Deserializing untrusted data
ObjectInputStream ois = new ObjectInputStream(inputStream);
Object obj = ois.readObject();  // Can execute arbitrary code

// Good: Validate class before deserialization
ObjectInputStream ois = new ObjectInputStream(inputStream) {
    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException {
        if (!desc.getName().equals("com.myapp.SafeClass")) {
            throw new InvalidClassException("Unauthorized deserialization");
        }
        return super.resolveClass(desc);
    }
};

// Better: Use safe formats like JSON
ObjectMapper mapper = new ObjectMapper();
User user = mapper.readValue(jsonString, User.class);
```

### Path Traversal Prevention

```java
// Bad: Direct file access
String filename = request.getParameter("file");
File file = new File("/var/uploads/" + filename);
FileInputStream fis = new FileInputStream(file);

// Good: Validate canonical path
String filename = request.getParameter("file");
File baseDir = new File("/var/uploads/");
File file = new File(baseDir, filename);

String canonicalPath = file.getCanonicalPath();
String basePath = baseDir.getCanonicalPath();

if (!canonicalPath.startsWith(basePath)) {
    throw new SecurityException("Invalid file path");
}

FileInputStream fis = new FileInputStream(file);
```

### Secure Random Generation

```java
// Bad: Predictable random
Random random = new Random();
int token = random.nextInt();  // NOT cryptographically secure

// Good: SecureRandom
SecureRandom secureRandom = new SecureRandom();
byte[] token = new byte[32];
secureRandom.nextBytes(token);

// Convert to hex string
String tokenString = bytesToHex(token);
```

### Password Hashing

```java
// Bad: Plain MD5/SHA1
MessageDigest md = MessageDigest.getInstance("MD5");
byte[] hash = md.digest(password.getBytes());

// Good: bcrypt with Spring Security
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

BCryptPasswordEncoder encoder = new BCryptPasswordEncoder(12);  // Work factor 12
String hashedPassword = encoder.encode(password);

boolean matches = encoder.matches(rawPassword, hashedPassword);
```

### LDAP Injection Prevention

```java
// Bad: String concatenation
String filter = "(uid=" + username + ")";
NamingEnumeration<SearchResult> results = ctx.search("ou=users", filter, null);

// Good: Escape special characters
public static String escapeLDAPSearchFilter(String filter) {
    StringBuilder sb = new StringBuilder();
    for (char c : filter.toCharArray()) {
        switch (c) {
            case '\\': sb.append("\\5c"); break;
            case '*':  sb.append("\\2a"); break;
            case '(':  sb.append("\\28"); break;
            case ')':  sb.append("\\29"); break;
            case '\0': sb.append("\\00"); break;
            default: sb.append(c);
        }
    }
    return sb.toString();
}

String filter = "(uid=" + escapeLDAPSearchFilter(username) + ")";
```

### Thread Safety

```java
// Bad: Mutable shared state
public class Counter {
    private int count = 0;

    public void increment() {
        count++;  // Race condition
    }
}

// Good: Synchronized method
public class Counter {
    private int count = 0;

    public synchronized void increment() {
        count++;
    }
}

// Better: AtomicInteger
import java.util.concurrent.atomic.AtomicInteger;

public class Counter {
    private AtomicInteger count = new AtomicInteger(0);

    public void increment() {
        count.incrementAndGet();
    }
}
```

---

## Security Headers

### HTTP Security Headers Configuration

**Python (Flask):**
```python
from flask import Flask
from flask_talisman import Talisman

app = Flask(__name__)

# Configure security headers
Talisman(app,
    force_https=True,
    strict_transport_security=True,
    content_security_policy={
        'default-src': "'self'",
        'script-src': "'self'",
        'style-src': "'self' 'unsafe-inline'",
        'img-src': "'self' data:",
    },
    content_security_policy_nonce_in=['script-src'],
    x_content_type_options=True,
    x_frame_options='DENY',
    x_xss_protection=True
)
```

**JavaScript (Express):**
```javascript
const helmet = require('helmet');
const app = express();

app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            scriptSrc: ["'self'"],
            styleSrc: ["'self'", "'unsafe-inline'"],
            imgSrc: ["'self'", "data:"],
        },
    },
    hsts: {
        maxAge: 31536000,
        includeSubDomains: true,
        preload: true
    },
    frameguard: { action: 'deny' },
    noSniff: true,
    xssFilter: true
}));
```

**Java (Spring Boot):**
```java
@Configuration
@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter {

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.headers()
            .contentSecurityPolicy("default-src 'self'")
            .and()
            .httpStrictTransportSecurity()
                .maxAgeInSeconds(31536000)
                .includeSubDomains(true)
            .and()
            .frameOptions().deny()
            .and()
            .xssProtection()
            .and()
            .contentTypeOptions();
    }
}
```

---

## Common Security Pitfalls

### 1. Trusting User Input
Never trust data from users, APIs, or any external source. Always validate and sanitize.

### 2. Security by Obscurity
Don't rely on hiding implementation details. Use proper security controls.

### 3. Hardcoded Secrets
Never commit secrets to version control. Use environment variables or secret managers.

### 4. Insufficient Logging
Log security events for audit trails and incident response.

### 5. Missing Error Handling
Unhandled errors can leak sensitive information.

### 6. Outdated Dependencies
Regularly update and scan dependencies for vulnerabilities.

### 7. Weak Cryptography
Use industry-standard algorithms and sufficient key lengths.

### 8. Client-Side Security
Never trust client-side validation. Always validate on server.

---

## Secure Code Review Checklist

- [ ] All user inputs validated and sanitized
- [ ] SQL queries use parameterized statements
- [ ] No command injection vulnerabilities
- [ ] No path traversal vulnerabilities
- [ ] Passwords hashed with strong algorithm (bcrypt/Argon2)
- [ ] Secrets stored securely, not hardcoded
- [ ] HTTPS enforced for all connections
- [ ] Security headers configured
- [ ] No sensitive data in logs
- [ ] Error messages don't reveal system details
- [ ] Authentication/authorization properly implemented
- [ ] Session management secure
- [ ] CSRF protection enabled
- [ ] XSS prevention measures in place
- [ ] Dependencies up to date
- [ ] Secure random number generation
- [ ] No insecure deserialization
- [ ] Rate limiting on sensitive endpoints
- [ ] Audit logging for security events
