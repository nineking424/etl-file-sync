# OWASP Top 10 Security Vulnerabilities

Comprehensive guide to the OWASP Top 10 web application security risks with detection and remediation strategies.

## A01: Broken Access Control

### Description
Restrictions on what authenticated users can do are not properly enforced. Attackers can exploit these flaws to access unauthorized functionality or data.

### Common Vulnerabilities
- Missing authorization checks
- Insecure direct object references (IDOR)
- Privilege escalation
- Forced browsing to authenticated pages
- CORS misconfiguration

### Detection Patterns

**Python (Flask):**
```python
# VULNERABLE: No authorization check
@app.route('/admin/users/<user_id>/delete')
def delete_user(user_id):
    user = User.query.get(user_id)
    db.session.delete(user)
    db.session.commit()
    return {'status': 'deleted'}

# SECURE: Authorization enforced
@app.route('/admin/users/<user_id>/delete')
@require_admin
def delete_user(user_id):
    if not current_user.is_admin:
        abort(403)
    user = User.query.get(user_id)
    db.session.delete(user)
    db.session.commit()
    return {'status': 'deleted'}
```

**JavaScript (Express):**
```javascript
// VULNERABLE: Direct object reference
app.get('/api/documents/:id', async (req, res) => {
    const doc = await Document.findById(req.params.id);
    res.json(doc);
});

// SECURE: Ownership verification
app.get('/api/documents/:id', authenticateUser, async (req, res) => {
    const doc = await Document.findById(req.params.id);
    if (!doc) return res.status(404).json({ error: 'Not found' });
    if (doc.ownerId !== req.user.id && !req.user.isAdmin) {
        return res.status(403).json({ error: 'Forbidden' });
    }
    res.json(doc);
});
```

**Java (Spring):**
```java
// VULNERABLE: Missing authorization
@GetMapping("/api/users/{id}/profile")
public UserProfile getProfile(@PathVariable Long id) {
    return userService.getProfile(id);
}

// SECURE: Authorization annotation
@GetMapping("/api/users/{id}/profile")
@PreAuthorize("hasRole('ADMIN') or #id == authentication.principal.id")
public UserProfile getProfile(@PathVariable Long id) {
    return userService.getProfile(id);
}
```

### Remediation
1. Implement proper authorization framework
2. Deny by default, allow explicitly
3. Enforce access controls on server-side
4. Use indirect object references
5. Log access control failures
6. Rate limit API calls
7. Disable directory listing

---

## A02: Cryptographic Failures

### Description
Failures related to cryptography (or lack thereof) that expose sensitive data.

### Common Vulnerabilities
- Storing passwords in plain text
- Using weak encryption algorithms (MD5, SHA1, DES)
- Hardcoded encryption keys
- Transmitting sensitive data over HTTP
- Predictable cryptographic keys

### Detection Patterns

**Python:**
```python
# VULNERABLE: Weak hashing
import hashlib
password_hash = hashlib.md5(password.encode()).hexdigest()

# VULNERABLE: Hardcoded secret
SECRET_KEY = "my-secret-key-12345"

# SECURE: Strong password hashing
import bcrypt
salt = bcrypt.gensalt(rounds=12)
password_hash = bcrypt.hashpw(password.encode(), salt)

# SECURE: Environment variable
import os
SECRET_KEY = os.environ['SECRET_KEY']
```

**JavaScript:**
```javascript
// VULNERABLE: Weak encryption
const crypto = require('crypto');
const hash = crypto.createHash('md5').update(password).digest('hex');

// VULNERABLE: Insecure transmission
fetch('http://api.example.com/login', {
    method: 'POST',
    body: JSON.stringify({ password: userPassword })
});

// SECURE: Strong hashing
const bcrypt = require('bcrypt');
const saltRounds = 12;
const hash = await bcrypt.hash(password, saltRounds);

// SECURE: HTTPS only
fetch('https://api.example.com/login', {
    method: 'POST',
    body: JSON.stringify({ password: userPassword })
});
```

**Java:**
```java
// VULNERABLE: DES encryption
Cipher cipher = Cipher.getInstance("DES");

// VULNERABLE: Static key
private static final String ENCRYPTION_KEY = "MySecretKey123";

// SECURE: AES-256 with secure key
SecretKey key = KeyGenerator.getInstance("AES").generateKey();
Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
cipher.init(Cipher.ENCRYPT_MODE, key);

// Store key securely in key management system
```

### Remediation
1. Use strong encryption algorithms (AES-256, RSA-2048+)
2. Hash passwords with bcrypt, Argon2, or PBKDF2
3. Use proper key management (rotate keys, secure storage)
4. Enforce HTTPS/TLS for all data transmission
5. Don't roll your own crypto
6. Validate certificates properly

---

## A03: Injection

### Description
Hostile data is sent to an interpreter as part of a command or query, tricking the interpreter into executing unintended commands or accessing unauthorized data.

### Types
- SQL Injection
- NoSQL Injection
- OS Command Injection
- LDAP Injection
- XPath Injection
- Template Injection

### SQL Injection Detection

**Python:**
```python
# VULNERABLE: String concatenation
def get_user(username):
    query = f"SELECT * FROM users WHERE username = '{username}'"
    cursor.execute(query)

# VULNERABLE: String formatting
query = "SELECT * FROM users WHERE id = %s" % user_id

# SECURE: Parameterized queries
def get_user(username):
    query = "SELECT * FROM users WHERE username = ?"
    cursor.execute(query, (username,))

# SECURE: ORM (SQLAlchemy)
user = User.query.filter_by(username=username).first()
```

**JavaScript:**
```javascript
// VULNERABLE: String concatenation
const query = `SELECT * FROM products WHERE category = '${category}'`;
connection.query(query, callback);

// SECURE: Parameterized queries
const query = 'SELECT * FROM products WHERE category = ?';
connection.query(query, [category], callback);

// SECURE: ORM (Sequelize)
const products = await Product.findAll({ where: { category } });
```

**Java:**
```java
// VULNERABLE: String concatenation
String query = "SELECT * FROM users WHERE email = '" + email + "'";
Statement stmt = connection.createStatement();
ResultSet rs = stmt.executeQuery(query);

// SECURE: Prepared statements
String query = "SELECT * FROM users WHERE email = ?";
PreparedStatement pstmt = connection.prepareStatement(query);
pstmt.setString(1, email);
ResultSet rs = pstmt.executeQuery();

// SECURE: JPA/Hibernate
User user = entityManager.createQuery(
    "SELECT u FROM User u WHERE u.email = :email", User.class)
    .setParameter("email", email)
    .getSingleResult();
```

### Command Injection Detection

**Python:**
```python
# VULNERABLE: Shell injection
import os
filename = request.args.get('file')
os.system(f'cat {filename}')

# SECURE: Avoid shell, use safe alternatives
import subprocess
subprocess.run(['cat', filename], check=True)

# BETTER: Use built-in functions
with open(filename, 'r') as f:
    content = f.read()
```

**JavaScript:**
```javascript
// VULNERABLE: Command injection
const { exec } = require('child_process');
const filename = req.query.file;
exec(`cat ${filename}`, callback);

// SECURE: Avoid shell
const { execFile } = require('child_process');
execFile('cat', [filename], callback);

// BETTER: Use built-in functions
const fs = require('fs');
fs.readFile(filename, 'utf8', callback);
```

### XSS Detection

**JavaScript:**
```javascript
// VULNERABLE: Direct HTML insertion
document.getElementById('output').innerHTML = userInput;

// VULNERABLE: eval()
eval(userProvidedCode);

// SECURE: Text content
document.getElementById('output').textContent = userInput;

// SECURE: DOM API
const div = document.createElement('div');
div.textContent = userInput;
document.getElementById('output').appendChild(div);

// SECURE: Use framework escaping (React, Vue, Angular)
<div>{userInput}</div>  // Auto-escaped in React
```

**Python (Flask):**
```python
# VULNERABLE: Marking as safe
from flask import Markup
return Markup(user_input)

# SECURE: Auto-escaping (default in Jinja2)
return render_template('template.html', data=user_input)
```

**Java (JSP):**
```jsp
<!-- VULNERABLE: Unescaped output -->
<%= request.getParameter("name") %>

<!-- SECURE: JSTL with escaping -->
<c:out value="${param.name}" />
```

### Remediation
1. Use parameterized queries/prepared statements
2. Validate and sanitize all user input
3. Use ORM frameworks
4. Escape special characters
5. Use least privilege for database accounts
6. Implement WAF (Web Application Firewall)

---

## A04: Insecure Design

### Description
Missing or ineffective security controls in the design phase.

### Common Issues
- Lack of threat modeling
- Missing security requirements
- Insecure architecture patterns
- Lack of security controls for business logic

### Examples

**Missing Rate Limiting:**
```python
# VULNERABLE: No rate limiting
@app.route('/api/login', methods=['POST'])
def login():
    username = request.json['username']
    password = request.json['password']
    if authenticate(username, password):
        return {'token': generate_token(username)}
    return {'error': 'Invalid credentials'}, 401

# SECURE: Rate limiting implemented
from flask_limiter import Limiter

limiter = Limiter(app, key_func=lambda: request.remote_addr)

@app.route('/api/login', methods=['POST'])
@limiter.limit("5 per minute")
def login():
    username = request.json['username']
    password = request.json['password']
    if authenticate(username, password):
        return {'token': generate_token(username)}
    return {'error': 'Invalid credentials'}, 401
```

### Remediation
1. Perform threat modeling
2. Define security requirements early
3. Use secure design patterns
4. Implement defense in depth
5. Principle of least privilege

---

## A05: Security Misconfiguration

### Description
Missing security hardening, unnecessary features enabled, default accounts unchanged.

### Detection Patterns

**Verbose Error Messages:**
```python
# VULNERABLE: Detailed errors in production
@app.errorhandler(Exception)
def handle_error(e):
    return str(e), 500  # Exposes stack trace

# SECURE: Generic error messages
@app.errorhandler(Exception)
def handle_error(e):
    logger.error(f"Error: {e}")  # Log details
    return "An error occurred", 500  # Generic message
```

**Default Credentials:**
```java
// VULNERABLE: Default admin account
if (username.equals("admin") && password.equals("admin123")) {
    return true;
}

// SECURE: Force password change on first login
if (user.isDefaultPassword()) {
    return new RequirePasswordChangeResponse();
}
```

### Remediation
1. Disable debug mode in production
2. Remove default accounts
3. Configure security headers
4. Keep software updated
5. Disable unnecessary features
6. Use security scanning tools

---

## A06: Vulnerable and Outdated Components

### Description
Using components with known vulnerabilities.

### Detection
```bash
# Python
pip list --outdated
safety check

# Node.js
npm audit
npm outdated

# Java
mvn dependency-check:check
```

### Remediation
1. Maintain inventory of components
2. Monitor CVE databases
3. Use dependency scanning tools
4. Keep dependencies updated
5. Remove unused dependencies
6. Subscribe to security bulletins

---

## A07: Identification and Authentication Failures

### Description
Broken authentication and session management.

### Detection Patterns

**Weak Password Policy:**
```python
# VULNERABLE: No password validation
def create_user(username, password):
    user = User(username=username, password=hash_password(password))
    db.session.add(user)

# SECURE: Strong password policy
import re

def validate_password(password):
    if len(password) < 12:
        raise ValueError("Password must be at least 12 characters")
    if not re.search(r'[A-Z]', password):
        raise ValueError("Password must contain uppercase letter")
    if not re.search(r'[a-z]', password):
        raise ValueError("Password must contain lowercase letter")
    if not re.search(r'[0-9]', password):
        raise ValueError("Password must contain number")
    if not re.search(r'[^A-Za-z0-9]', password):
        raise ValueError("Password must contain special character")
```

**Session Fixation:**
```python
# VULNERABLE: Same session ID after login
@app.route('/login', methods=['POST'])
def login():
    if authenticate(username, password):
        session['user_id'] = user.id
        return redirect('/')

# SECURE: Regenerate session ID
@app.route('/login', methods=['POST'])
def login():
    if authenticate(username, password):
        session.clear()
        session.regenerate()
        session['user_id'] = user.id
        return redirect('/')
```

### Remediation
1. Implement MFA
2. Strong password policies
3. Session timeout
4. Secure session management
5. Brute force protection
6. Account lockout policies

---

## A08-A10 Summary

### A08: Software and Data Integrity Failures
- Use code signing
- Verify package integrity
- Secure CI/CD pipelines
- Avoid insecure deserialization

### A09: Security Logging and Monitoring Failures
- Log security events
- Monitor for anomalies
- Alerting system
- Incident response plan
- Protect logs from tampering

### A10: Server-Side Request Forgery (SSRF)
- Validate and sanitize URLs
- Use allowlists for domains
- Disable unnecessary protocols
- Network segmentation

---

## Summary Checklist

- [ ] Authorization checks on all protected resources (A01)
- [ ] Strong encryption and secure key management (A02)
- [ ] Parameterized queries to prevent injection (A03)
- [ ] Security requirements in design phase (A04)
- [ ] Production hardening and secure defaults (A05)
- [ ] Dependencies up to date and scanned (A06)
- [ ] Strong authentication and session management (A07)
- [ ] Code signing and integrity verification (A08)
- [ ] Comprehensive security logging (A09)
- [ ] SSRF protection on external requests (A10)
