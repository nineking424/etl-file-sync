# Networking Patterns

Comprehensive guide to Kubernetes networking including services, ingress, and network policies.

## Service Types

### ClusterIP (Default)
```yaml
apiVersion: v1
kind: Service
metadata:
  name: web-service
spec:
  type: ClusterIP
  selector:
    app: web
  ports:
  - port: 80
    targetPort: 8080
    protocol: TCP

# Internal-only access
# Assigned cluster-internal IP
# Use for: Service-to-service communication
```

### NodePort
```yaml
apiVersion: v1
kind: Service
metadata:
  name: web-nodeport
spec:
  type: NodePort
  selector:
    app: web
  ports:
  - port: 80
    targetPort: 8080
    nodePort: 30080  # Optional: 30000-32767
    protocol: TCP

# Accessible on <NodeIP>:<NodePort>
# Use for: Development, testing, simple external access
```

### LoadBalancer
```yaml
apiVersion: v1
kind: Service
metadata:
  name: web-lb
spec:
  type: LoadBalancer
  selector:
    app: web
  ports:
  - port: 80
    targetPort: 8080
  loadBalancerSourceRanges:
  - "10.0.0.0/8"  # Restrict source IPs

# Cloud provider creates external load balancer
# Use for: Production external access
```

### Headless Service
```yaml
apiVersion: v1
kind: Service
metadata:
  name: database-headless
spec:
  clusterIP: None  # Headless
  selector:
    app: database
  ports:
  - port: 5432

# No load balancing, returns pod IPs directly
# Use for: StatefulSets, direct pod access, service discovery
```

## Ingress Patterns

### Basic HTTP Ingress
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: web-ingress
spec:
  ingressClassName: nginx
  rules:
  - host: example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: web-service
            port:
              number: 80
```

### TLS/HTTPS Ingress
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: secure-ingress
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
spec:
  ingressClassName: nginx
  tls:
  - hosts:
    - example.com
    secretName: example-tls
  rules:
  - host: example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: web-service
            port:
              number: 80
```

### Path-Based Routing
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: path-routing
spec:
  rules:
  - host: api.example.com
    http:
      paths:
      - path: /v1
        pathType: Prefix
        backend:
          service:
            name: api-v1
            port:
              number: 80
      - path: /v2
        pathType: Prefix
        backend:
          service:
            name: api-v2
            port:
              number: 80
```

## Network Policies

### Default Deny All
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: production
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress

# Denies all traffic by default
# Add allow policies selectively
```

### Allow Specific Ingress
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-web-ingress
spec:
  podSelector:
    matchLabels:
      app: web
  policyTypes:
  - Ingress
  ingress:
  # Allow from ingress controller
  - from:
    - namespaceSelector:
        matchLabels:
          name: ingress-nginx
      podSelector:
        matchLabels:
          app: ingress-nginx
    ports:
    - protocol: TCP
      port: 8080
```

### Allow Specific Egress
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-web-egress
spec:
  podSelector:
    matchLabels:
      app: web
  policyTypes:
  - Egress
  egress:
  # Allow to database
  - to:
    - podSelector:
        matchLabels:
          app: database
    ports:
    - protocol: TCP
      port: 5432
  # Allow DNS
  - to:
    - namespaceSelector:
        matchLabels:
          name: kube-system
    ports:
    - protocol: UDP
      port: 53
```

## DNS and Service Discovery

### Service DNS
```bash
# Service DNS format
<service-name>.<namespace>.svc.cluster.local

# Examples
web-service.production.svc.cluster.local
database.default.svc.cluster.local

# Short forms (within same namespace)
web-service
web-service.production
```

### Headless Service DNS
```bash
# Returns all pod IPs
<pod-name>.<service-name>.<namespace>.svc.cluster.local

# Example for StatefulSet
web-0.web-service.production.svc.cluster.local
web-1.web-service.production.svc.cluster.local
```

## Best Practices

1. **Use ClusterIP** for internal services
2. **Implement NetworkPolicies** for zero-trust security
3. **Use Ingress** for HTTP/HTTPS routing
4. **Enable TLS** for external endpoints
5. **Use headless services** for StatefulSets
6. **Monitor network latency** between services
7. **Document DNS names** for service discovery
8. **Test network policies** in staging
9. **Use session affinity** when needed
10. **Implement proper egress controls**
