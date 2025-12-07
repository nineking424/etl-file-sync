---
name: security-auditor
description: Identify security vulnerabilities and provide remediation guidance following OWASP standards and secure coding best practices
---

# Security Auditor Skill

## Overview

The Security Auditor skill helps you identify security vulnerabilities in code and provides actionable remediation guidance. It follows OWASP Top 10 standards, applies secure coding best practices, and detects common security anti-patterns across multiple programming languages.

## Core Capabilities

### 1. OWASP Top 10 Compliance Check
- **A01: Broken Access Control**: Detect missing authorization checks, insecure direct object references
- **A02: Cryptographic Failures**: Identify weak encryption, hardcoded secrets, insecure storage
- **A03: Injection**: Find SQL injection, command injection, XSS vulnerabilities
- **A04: Insecure Design**: Detect missing security controls in architecture
- **A05: Security Misconfiguration**: Identify default credentials, verbose errors, open ports
- **A06: Vulnerable Components**: Flag outdated dependencies with known CVEs
- **A07: Authentication Failures**: Find weak password policies, session management issues
- **A08: Software/Data Integrity**: Detect unsigned updates, insecure deserialization
- **A09: Logging/Monitoring Failures**: Check for inadequate security logging
- **A10: SSRF**: Identify server-side request forgery vulnerabilities

### 2. Injection Attack Detection
- SQL Injection patterns (string concatenation in queries)
- Cross-Site Scripting (XSS) - reflected, stored, DOM-based
- Command Injection (shell command execution)
- LDAP Injection
- XML/XXE Injection
- Template Injection

### 3. Secret Detection
- Hardcoded passwords and API keys
- Exposed credentials in configuration files
- Private keys in source code
- Database connection strings
- Cloud provider credentials (AWS, Azure, GCP)

### 4. Dependency Vulnerability Analysis
- Identify outdated packages with known CVEs
- Check for vulnerable library versions
- Detect transitive dependency risks
- Recommend secure alternatives

### 5. Authentication & Authorization
- Weak password validation
- Missing multi-factor authentication
- Insecure session management
- Broken access control
- Missing CSRF protection

## Workflow

When performing a security audit:

### 1. Scope Analysis
- Identify entry points (API endpoints, forms, file uploads)
- Map data flow from input to storage
- List external dependencies and integrations
- Document authentication/authorization mechanisms

### 2. Automated Scanning
- Run static analysis for common vulnerabilities
- Check dependencies against CVE databases
- Scan for hardcoded secrets and credentials
- Analyze configuration files for misconfigurations

### 3. Manual Code Review
- Review authentication/authorization logic
- Examine input validation and sanitization
- Check cryptographic implementations
- Analyze error handling and logging

### 4. Vulnerability Classification
Classify findings by severity:
- **Critical**: Immediate exploitation possible, high impact
- **High**: Exploitation likely, significant impact
- **Medium**: Exploitation possible with effort, moderate impact
- **Low**: Limited exploitability or impact
- **Info**: No immediate risk, best practice recommendation

### 5. Remediation Guidance
For each vulnerability:
- Explain the security risk
- Show vulnerable code example
- Provide secure code alternative
- Reference relevant standards (OWASP, CWE)
- Suggest verification steps

## Security Audit Checklist

### Input Validation
- [ ] All user inputs are validated and sanitized
- [ ] Whitelist validation is used where possible
- [ ] Input length limits are enforced
- [ ] Special characters are properly escaped
- [ ] File uploads are restricted by type and size
- [ ] Content-Type headers are validated

### Authentication & Authorization
- [ ] Strong password policy enforced (length, complexity)
- [ ] Multi-factor authentication available
- [ ] Session tokens are securely generated
- [ ] Sessions expire after inactivity
- [ ] Authorization checks on all protected resources
- [ ] Principle of least privilege applied

### Cryptography
- [ ] Sensitive data encrypted at rest and in transit
- [ ] Strong encryption algorithms used (AES-256, RSA-2048+)
- [ ] No hardcoded encryption keys or secrets
- [ ] Proper key management and rotation
- [ ] Secure random number generation
- [ ] Passwords hashed with bcrypt/Argon2/PBKDF2

### Data Protection
- [ ] SQL queries use parameterized statements
- [ ] Output encoding prevents XSS
- [ ] CSRF tokens on state-changing operations
- [ ] Sensitive data not logged or exposed in errors
- [ ] PII handled according to regulations (GDPR, CCPA)
- [ ] Secure data deletion when no longer needed

### Configuration & Deployment
- [ ] Default credentials changed
- [ ] Debug mode disabled in production
- [ ] Verbose error messages disabled
- [ ] Security headers configured (CSP, HSTS, X-Frame-Options)
- [ ] HTTPS enforced, HTTP redirected
- [ ] Dependencies up to date

### Logging & Monitoring
- [ ] Security events logged (auth failures, access denied)
- [ ] Logs don't contain sensitive data
- [ ] Log tampering prevented
- [ ] Alerts configured for suspicious activity
- [ ] Audit trail for critical operations
- [ ] Log retention policy defined

## Usage Examples

### Example 1: SQL Injection Detection

**Vulnerable Code (Python):**
```python
def get_user(username):
    query = "SELECT * FROM users WHERE username = '" + username + "'"
    cursor.execute(query)
    return cursor.fetchone()
```

**Security Issue:**
- **Vulnerability**: SQL Injection (OWASP A03)
- **Severity**: Critical
- **CWE**: CWE-89 (Improper Neutralization of Special Elements)
- **Risk**: Attacker can execute arbitrary SQL, steal/modify data, bypass authentication

**Remediation:**
```python
def get_user(username):
    query = "SELECT * FROM users WHERE username = ?"
    cursor.execute(query, (username,))
    return cursor.fetchone()
```

### Example 2: XSS Prevention

**Vulnerable Code (JavaScript):**
```javascript
function displayComment(comment) {
    document.getElementById('comments').innerHTML += comment;
}
```

**Security Issue:**
- **Vulnerability**: Cross-Site Scripting (OWASP A03)
- **Severity**: High
- **CWE**: CWE-79 (Improper Neutralization of Input)
- **Risk**: Attacker can inject malicious scripts, steal session cookies, deface page

**Remediation:**
```javascript
function displayComment(comment) {
    const div = document.createElement('div');
    div.textContent = comment; // Auto-escapes HTML
    document.getElementById('comments').appendChild(div);
}

// Or use a templating library with auto-escaping
```

### Example 3: Hardcoded Secrets Detection

**Vulnerable Code (Java):**
```java
public class DatabaseConfig {
    private static final String DB_PASSWORD = "MySecretPass123!";
    private static final String API_KEY = "sk_live_1234567890abcdef";
}
```

**Security Issue:**
- **Vulnerability**: Hardcoded Credentials (OWASP A02)
- **Severity**: Critical
- **CWE**: CWE-798 (Use of Hard-coded Credentials)
- **Risk**: Credentials exposed in source control, difficult to rotate, potential data breach

**Remediation:**
```java
public class DatabaseConfig {
    private final String dbPassword;
    private final String apiKey;

    public DatabaseConfig() {
        // Load from environment variables
        this.dbPassword = System.getenv("DB_PASSWORD");
        this.apiKey = System.getenv("API_KEY");

        // Or use a secrets manager (AWS Secrets Manager, HashiCorp Vault)
        // this.dbPassword = secretsManager.getSecret("db-password");
    }
}
```

### Example 4: Weak Cryptography

**Vulnerable Code (Python):**
```python
import hashlib

def hash_password(password):
    return hashlib.md5(password.encode()).hexdigest()
```

**Security Issue:**
- **Vulnerability**: Weak Cryptographic Algorithm (OWASP A02)
- **Severity**: High
- **CWE**: CWE-327 (Use of Broken Cryptographic Algorithm)
- **Risk**: MD5 is cryptographically broken, passwords can be cracked with rainbow tables

**Remediation:**
```python
import bcrypt

def hash_password(password):
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode(), salt)

def verify_password(password, hashed):
    return bcrypt.checkpw(password.encode(), hashed)
```

### Example 5: Missing Authorization

**Vulnerable Code (Node.js/Express):**
```javascript
app.delete('/api/users/:id', (req, res) => {
    const userId = req.params.id;
    deleteUser(userId);
    res.json({ success: true });
});
```

**Security Issue:**
- **Vulnerability**: Broken Access Control (OWASP A01)
- **Severity**: Critical
- **CWE**: CWE-862 (Missing Authorization)
- **Risk**: Any authenticated user can delete any other user

**Remediation:**
```javascript
app.delete('/api/users/:id', authenticateUser, (req, res) => {
    const userId = req.params.id;
    const currentUser = req.user;

    // Check if user is admin or deleting their own account
    if (!currentUser.isAdmin && currentUser.id !== userId) {
        return res.status(403).json({ error: 'Forbidden' });
    }

    deleteUser(userId);
    res.json({ success: true });
});
```

## Integration Points

### With Code Reviewer
- Security Auditor focuses on security vulnerabilities
- Code Reviewer handles code quality and maintainability
- Both can identify similar issues from different perspectives

### With Tester
- Security tests should be added to test suites
- Penetration testing scenarios for critical flows
- Negative test cases for security controls

### With Document Maintainer
- Security audit reports should be documented
- Vulnerability remediation tracked
- Security requirements updated

## Security Scanning Tools

### Static Analysis (SAST)
- **Python**: Bandit, Safety, Semgrep
- **JavaScript**: ESLint security plugins, NodeJsScan
- **Java**: SpotBugs, FindSecBugs, SonarQube
- **Multi-language**: Semgrep, SonarQube, Checkmarx

### Dependency Scanning
- **npm**: npm audit, Snyk
- **pip**: Safety, pip-audit
- **Maven/Gradle**: OWASP Dependency-Check
- **Multi-language**: Dependabot, Snyk, WhiteSource

### Secret Scanning
- git-secrets
- TruffleHog
- Gitleaks
- detect-secrets

### Dynamic Analysis (DAST)
- OWASP ZAP
- Burp Suite
- Nikto
- SQLMap

## Best Practices

1. **Shift Left**: Integrate security early in development lifecycle
2. **Defense in Depth**: Multiple layers of security controls
3. **Principle of Least Privilege**: Minimal permissions required
4. **Fail Securely**: Errors should not expose sensitive information
5. **Don't Trust Input**: Validate and sanitize all external input
6. **Use Security Libraries**: Don't roll your own crypto
7. **Keep Dependencies Updated**: Regularly patch vulnerabilities
8. **Security Testing**: Include security tests in CI/CD
9. **Code Review**: Security-focused peer review
10. **Continuous Monitoring**: Monitor for security events in production

## Reporting Format

### Vulnerability Report Structure
```markdown
## Vulnerability: [Name]

**Severity**: Critical/High/Medium/Low/Info
**OWASP Category**: [A01-A10]
**CWE**: [CWE-XXX]
**Location**: [File:Line]

**Description**:
[Explain the vulnerability and its impact]

**Proof of Concept**:
[Show how to exploit the vulnerability]

**Remediation**:
[Provide step-by-step fix]

**Code Example**:
```language
// Secure implementation
```

**Verification**:
[How to verify the fix]

**References**:
- [OWASP Link]
- [CWE Link]
- [Additional Resources]
```

## References

See the `references/` directory for detailed guides on:
- **owasp-top10.md**: Comprehensive OWASP Top 10 vulnerability guide
- **secure-coding.md**: Secure coding practices by language
- **vulnerability-patterns.md**: Common vulnerability patterns and detection
- **remediation-guide.md**: Step-by-step remediation for common vulnerabilities
