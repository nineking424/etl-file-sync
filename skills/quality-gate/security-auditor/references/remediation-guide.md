# Security Vulnerability Remediation Guide

Step-by-step remediation instructions for common security vulnerabilities.

## SQL Injection Remediation

### Step 1: Identify Vulnerable Code
Look for string concatenation or formatting in SQL queries.

### Step 2: Use Parameterized Queries

**Python:**
```python
# Before (Vulnerable)
def get_user(email):
    query = f"SELECT * FROM users WHERE email = '{email}'"
    return cursor.execute(query).fetchone()

# After (Secure)
def get_user(email):
    query = "SELECT * FROM users WHERE email = ?"
    return cursor.execute(query, (email,)).fetchone()

# Best: Use ORM
from sqlalchemy import select
def get_user(email):
    return session.execute(
        select(User).where(User.email == email)
    ).scalar_one_or_none()
```

**JavaScript:**
```javascript
// Before (Vulnerable)
const query = `SELECT * FROM users WHERE email = '${email}'`;
connection.query(query, callback);

// After (Secure)
const query = 'SELECT * FROM users WHERE email = ?';
connection.query(query, [email], callback);

// Best: Use ORM
const user = await User.findOne({ where: { email } });
```

**Java:**
```java
// Before (Vulnerable)
String query = "SELECT * FROM users WHERE email = '" + email + "'";
Statement stmt = connection.createStatement();
ResultSet rs = stmt.executeQuery(query);

// After (Secure)
String query = "SELECT * FROM users WHERE email = ?";
PreparedStatement pstmt = connection.prepareStatement(query);
pstmt.setString(1, email);
ResultSet rs = pstmt.executeQuery();

// Best: Use JPA
@Query("SELECT u FROM User u WHERE u.email = :email")
User findByEmail(@Param("email") String email);
```

### Step 3: Test the Fix
```python
# Test with malicious input
malicious_input = "'; DROP TABLE users; --"
result = get_user(malicious_input)
# Should return no results, not execute DROP TABLE
```

### Step 4: Scan for Similar Issues
```bash
# Use grep to find other potential SQL injection points
grep -r "execute.*format\|execute.*%" .
grep -r "query.*\+" .
```

---

## XSS (Cross-Site Scripting) Remediation

### Step 1: Identify Output Points
Find where user input is rendered in HTML.

### Step 2: Apply Output Encoding

**JavaScript:**
```javascript
// Before (Vulnerable)
document.getElementById('output').innerHTML = userInput;

// After (Secure) - Option 1: Use textContent
document.getElementById('output').textContent = userInput;

// After (Secure) - Option 2: Create element
const div = document.createElement('div');
div.textContent = userInput;
document.getElementById('output').appendChild(div);

// For HTML content: Sanitize with DOMPurify
import DOMPurify from 'dompurify';
const clean = DOMPurify.sanitize(dirtyHTML);
element.innerHTML = clean;
```

**Python (Flask/Jinja2):**
```python
# Before (Vulnerable)
from flask import Markup
return Markup(user_input)

# After (Secure) - Auto-escaping enabled by default
return render_template('page.html', content=user_input)

# In template (Jinja2 auto-escapes by default)
{{ content }}  <!-- Automatically escaped -->

# For trusted HTML only:
{{ content | safe }}  <!-- Use sparingly, only with trusted content -->
```

**Java (JSP):**
```jsp
<!-- Before (Vulnerable) -->
<%= request.getParameter("name") %>

<!-- After (Secure) - Use JSTL -->
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<c:out value="${param.name}" />

<!-- Or use fn:escapeXml -->
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
${fn:escapeXml(param.name)}
```

### Step 3: Implement Content Security Policy

**HTTP Header:**
```
Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:
```

**Python (Flask):**
```python
from flask import Flask
from flask_talisman import Talisman

app = Flask(__name__)
Talisman(app, content_security_policy={
    'default-src': "'self'",
    'script-src': "'self'",
})
```

**JavaScript (Express):**
```javascript
const helmet = require('helmet');
app.use(helmet.contentSecurityPolicy({
    directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'"],
    }
}));
```

### Step 4: Verify Fix
```javascript
// Test with XSS payload
const xssPayload = '<script>alert("XSS")</script>';
// Should be displayed as text, not executed
```

---

## Hardcoded Secrets Remediation

### Step 1: Identify Hardcoded Secrets
```bash
# Search for common patterns
grep -r "password\s*=\s*['\"]" .
grep -r "api_key\s*=\s*['\"]" .
grep -r "secret\s*=\s*['\"]" .
grep -r "token\s*=\s*['\"]" .

# Use specialized tools
trufflehog --regex --entropy=False .
gitleaks detect
```

### Step 2: Move to Environment Variables

**Python:**
```python
# Before (Vulnerable)
DATABASE_PASSWORD = "MySecretPassword123"
API_KEY = "sk_live_abc123xyz789"

# After (Secure)
import os

DATABASE_PASSWORD = os.environ['DATABASE_PASSWORD']
API_KEY = os.environ['API_KEY']

# With defaults for development
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD', 'dev_password')

# Better: Use python-dotenv for local development
from dotenv import load_dotenv
load_dotenv()  # Load from .env file

DATABASE_PASSWORD = os.environ['DATABASE_PASSWORD']
```

**JavaScript:**
```javascript
// Before (Vulnerable)
const DB_PASSWORD = 'MySecretPassword123';
const API_KEY = 'sk_live_abc123xyz789';

// After (Secure)
const DB_PASSWORD = process.env.DB_PASSWORD;
const API_KEY = process.env.API_KEY;

// Better: Use dotenv for local development
require('dotenv').config();
const DB_PASSWORD = process.env.DB_PASSWORD;
```

**Java:**
```java
// Before (Vulnerable)
private static final String DB_PASSWORD = "MySecretPassword123";

// After (Secure)
private final String dbPassword = System.getenv("DB_PASSWORD");

// Better: Use Spring @Value
@Value("${database.password}")
private String dbPassword;
```

### Step 3: Create .env Template
```bash
# .env.example (commit this)
DATABASE_PASSWORD=your_password_here
API_KEY=your_api_key_here
AWS_ACCESS_KEY_ID=your_aws_key_here
```

### Step 4: Update .gitignore
```
# .gitignore
.env
.env.local
config/secrets.yml
credentials.json
```

### Step 5: Rotate Compromised Secrets
1. Generate new secrets/API keys
2. Update environment variables
3. Revoke old secrets
4. Test application with new secrets

### Step 6: Clean Git History (if secrets were committed)
```bash
# Use BFG Repo-Cleaner
git clone --mirror git://example.com/repo.git
java -jar bfg.jar --replace-text passwords.txt repo.git
cd repo.git
git reflog expire --expire=now --all
git gc --prune=now --aggressive
git push --force

# Or use git filter-branch
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch path/to/secrets.file" \
  --prune-empty --tag-name-filter cat -- --all
```

---

## Weak Cryptography Remediation

### Step 1: Replace Weak Hashing Algorithms

**Python:**
```python
# Before (Vulnerable)
import hashlib
password_hash = hashlib.md5(password.encode()).hexdigest()

# After (Secure)
import bcrypt

# Hashing
salt = bcrypt.gensalt(rounds=12)  # Work factor 12
password_hash = bcrypt.hashpw(password.encode(), salt)

# Verification
is_valid = bcrypt.checkpw(password.encode(), stored_hash)
```

**JavaScript:**
```javascript
// Before (Vulnerable)
const crypto = require('crypto');
const hash = crypto.createHash('md5').update(password).digest('hex');

// After (Secure)
const bcrypt = require('bcrypt');
const saltRounds = 12;

// Hashing
const hash = await bcrypt.hash(password, saltRounds);

// Verification
const isValid = await bcrypt.compare(password, storedHash);
```

**Java:**
```java
// Before (Vulnerable)
MessageDigest md = MessageDigest.getInstance("MD5");
byte[] hash = md.digest(password.getBytes());

// After (Secure) - Spring Security BCrypt
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

BCryptPasswordEncoder encoder = new BCryptPasswordEncoder(12);

// Hashing
String hash = encoder.encode(password);

// Verification
boolean isValid = encoder.matches(password, storedHash);
```

### Step 2: Replace Weak Encryption

**Python:**
```python
# Before (Vulnerable)
from Crypto.Cipher import DES
cipher = DES.new(key, DES.MODE_ECB)

# After (Secure)
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Protocol.KDF import PBKDF2

# Generate key from password
password = b"user_password"
salt = get_random_bytes(16)
key = PBKDF2(password, salt, dkLen=32)  # 256-bit key

# Encrypt
cipher = AES.new(key, AES.MODE_GCM)
nonce = cipher.nonce
ciphertext, tag = cipher.encrypt_and_digest(data)

# Decrypt
cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
plaintext = cipher.decrypt_and_verify(ciphertext, tag)
```

**Java:**
```java
// Before (Vulnerable)
Cipher cipher = Cipher.getInstance("DES");

// After (Secure)
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

// Generate key
KeyGenerator keyGen = KeyGenerator.getInstance("AES");
keyGen.init(256);  // 256-bit AES
SecretKey key = keyGen.generateKey();

// Encrypt
Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
byte[] iv = new byte[12];
SecureRandom random = new SecureRandom();
random.nextBytes(iv);

GCMParameterSpec spec = new GCMParameterSpec(128, iv);
cipher.init(Cipher.ENCRYPT_MODE, key, spec);
byte[] ciphertext = cipher.doFinal(plaintext);

// Decrypt
cipher.init(Cipher.DECRYPT_MODE, key, spec);
byte[] decrypted = cipher.doFinal(ciphertext);
```

---

## Missing Authorization Remediation

### Step 1: Implement Authorization Checks

**Python (Flask):**
```python
# Before (Vulnerable)
@app.route('/api/users/<user_id>/delete', methods=['DELETE'])
def delete_user(user_id):
    user = User.query.get(user_id)
    db.session.delete(user)
    db.session.commit()
    return {'status': 'deleted'}

# After (Secure)
from functools import wraps
from flask import abort, g

def require_admin(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not g.current_user or not g.current_user.is_admin:
            abort(403)
        return f(*args, **kwargs)
    return decorated_function

def require_owner_or_admin(f):
    @wraps(f)
    def decorated_function(user_id, *args, **kwargs):
        if not g.current_user:
            abort(401)
        if g.current_user.id != int(user_id) and not g.current_user.is_admin:
            abort(403)
        return f(user_id, *args, **kwargs)
    return decorated_function

@app.route('/api/users/<user_id>/delete', methods=['DELETE'])
@require_admin
def delete_user(user_id):
    user = User.query.get_or_404(user_id)
    db.session.delete(user)
    db.session.commit()
    return {'status': 'deleted'}
```

**JavaScript (Express):**
```javascript
// Before (Vulnerable)
app.delete('/api/users/:id', async (req, res) => {
    await User.destroy({ where: { id: req.params.id } });
    res.json({ status: 'deleted' });
});

// After (Secure)
const requireAdmin = (req, res, next) => {
    if (!req.user || !req.user.isAdmin) {
        return res.status(403).json({ error: 'Forbidden' });
    }
    next();
};

const requireOwnerOrAdmin = (req, res, next) => {
    if (!req.user) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    if (req.user.id !== parseInt(req.params.id) && !req.user.isAdmin) {
        return res.status(403).json({ error: 'Forbidden' });
    }
    next();
};

app.delete('/api/users/:id', authenticateToken, requireAdmin, async (req, res) => {
    await User.destroy({ where: { id: req.params.id } });
    res.json({ status: 'deleted' });
});
```

**Java (Spring Security):**
```java
// Before (Vulnerable)
@DeleteMapping("/api/users/{id}")
public void deleteUser(@PathVariable Long id) {
    userService.delete(id);
}

// After (Secure)
@DeleteMapping("/api/users/{id}")
@PreAuthorize("hasRole('ADMIN')")
public void deleteUser(@PathVariable Long id) {
    userService.delete(id);
}

// Or custom authorization
@DeleteMapping("/api/users/{id}")
public void deleteUser(@PathVariable Long id, Authentication auth) {
    User currentUser = (User) auth.getPrincipal();

    if (!currentUser.isAdmin() && !currentUser.getId().equals(id)) {
        throw new AccessDeniedException("Forbidden");
    }

    userService.delete(id);
}
```

---

## Command Injection Remediation

### Step 1: Avoid Shell Execution

**Python:**
```python
# Before (Vulnerable)
import os
filename = request.args.get('file')
os.system(f'cat {filename}')

# After (Secure) - Option 1: Use subprocess without shell
import subprocess
subprocess.run(['cat', filename], check=True, shell=False)

# After (Secure) - Option 2: Use Python built-ins
with open(filename, 'r') as f:
    content = f.read()
```

**JavaScript:**
```javascript
// Before (Vulnerable)
const { exec } = require('child_process');
exec(`ping ${host}`, callback);

// After (Secure) - Option 1: Use execFile
const { execFile } = require('child_process');
execFile('ping', [host], callback);

// After (Secure) - Option 2: Validate input
const validHosts = ['localhost', 'example.com'];
if (!validHosts.includes(host)) {
    throw new Error('Invalid host');
}
execFile('ping', [host], callback);
```

### Step 2: Input Validation
```python
import re

def validate_filename(filename):
    # Only allow alphanumeric, dash, underscore, dot
    if not re.match(r'^[\w\-\.]+$', filename):
        raise ValueError('Invalid filename')

    # Prevent path traversal
    if '..' in filename or '/' in filename:
        raise ValueError('Invalid filename')

    return filename
```

---

## Path Traversal Remediation

**Python:**
```python
# Before (Vulnerable)
def read_file(filename):
    path = f"/var/uploads/{filename}"
    with open(path, 'r') as f:
        return f.read()

# After (Secure)
from pathlib import Path

def read_file(filename):
    base_dir = Path("/var/uploads").resolve()
    requested_path = (base_dir / filename).resolve()

    # Ensure file is within base directory
    try:
        requested_path.relative_to(base_dir)
    except ValueError:
        raise ValueError("Invalid file path")

    with open(requested_path, 'r') as f:
        return f.read()
```

**Java:**
```java
// Before (Vulnerable)
public String readFile(String filename) throws IOException {
    File file = new File("/var/uploads/" + filename);
    return Files.readString(file.toPath());
}

// After (Secure)
public String readFile(String filename) throws IOException {
    Path basePath = Paths.get("/var/uploads/").toRealPath();
    Path requestedPath = basePath.resolve(filename).toRealPath();

    if (!requestedPath.startsWith(basePath)) {
        throw new SecurityException("Invalid file path");
    }

    return Files.readString(requestedPath);
}
```

---

## Testing Remediation

### Security Test Cases

**SQL Injection Tests:**
```python
def test_sql_injection_prevention():
    # Test with malicious input
    malicious_inputs = [
        "' OR '1'='1",
        "'; DROP TABLE users; --",
        "admin'--",
        "1' UNION SELECT * FROM passwords--"
    ]

    for payload in malicious_inputs:
        result = get_user(payload)
        assert result is None or result.username != 'admin'
```

**XSS Tests:**
```javascript
test('prevents XSS injection', () => {
    const xssPayloads = [
        '<script>alert("XSS")</script>',
        '<img src=x onerror=alert("XSS")>',
        'javascript:alert("XSS")'
    ];

    xssPayloads.forEach(payload => {
        render(<Component input={payload} />);
        expect(screen.queryByText(/XSS/)).not.toBeInTheDocument();
    });
});
```

---

## Verification Checklist

After remediation, verify:
- [ ] Vulnerability is fixed in all instances
- [ ] Similar issues across codebase addressed
- [ ] Tests added for vulnerability
- [ ] Security scan passes
- [ ] Code review completed
- [ ] Documentation updated
- [ ] Secrets rotated (if applicable)
- [ ] Deployment tested
