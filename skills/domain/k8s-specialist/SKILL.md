---
name: k8s-specialist
description: Design and manage Kubernetes resources and Helm charts including deployments, services, RBAC, HPA/VPA, and networking configurations
---

# Kubernetes Specialist Skill

## Overview

The Kubernetes Specialist skill provides expert guidance for designing, deploying, and managing Kubernetes resources. This skill covers deployment strategies, service configurations, Helm chart development, resource optimization, security policies, and advanced networking patterns for production-ready Kubernetes environments.

## Core Capabilities

### 1. Deployment and Service Creation
- Design deployment strategies (rolling update, blue/green, canary)
- Configure pod specifications with proper resource allocation
- Implement health checks (liveness, readiness, startup probes)
- Design service types (ClusterIP, NodePort, LoadBalancer, ExternalName)
- Configure ingress controllers and routing rules

### 2. Helm Chart Structuring
- Design modular, reusable Helm charts
- Implement values hierarchy and templating best practices
- Create chart dependencies and subcharts
- Design upgrade and rollback strategies
- Implement hooks for lifecycle management

### 3. Resource Request/Limit Optimization
- Calculate appropriate CPU and memory requests
- Set limits to prevent resource starvation
- Implement Quality of Service (QoS) classes
- Configure resource quotas and limit ranges
- Design vertical and horizontal scaling strategies

### 4. NetworkPolicy and RBAC Configuration
- Implement zero-trust network policies
- Design namespace isolation strategies
- Configure service accounts and roles
- Implement least-privilege RBAC policies
- Design security contexts and pod security policies

### 5. Autoscaling Configuration
- Configure Horizontal Pod Autoscaler (HPA) with custom metrics
- Implement Vertical Pod Autoscaler (VPA) for right-sizing
- Design cluster autoscaling strategies
- Configure KEDA for event-driven autoscaling
- Implement pod disruption budgets for availability

## Workflow

When working with Kubernetes resources:

### 1. Requirements Gathering
- Identify application resource requirements
- Determine scaling and availability needs
- Assess security and compliance requirements
- Evaluate network topology and access patterns
- Define monitoring and observability needs

### 2. Design Phase
- Select appropriate resource types (Deployment, StatefulSet, DaemonSet)
- Design service architecture and networking
- Plan resource allocation and QoS classes
- Design security policies (RBAC, network policies)
- Create Helm chart structure for templating

### 3. Implementation
- Create Kubernetes manifests or Helm charts
- Configure resource requests and limits
- Implement health checks and probes
- Set up autoscaling policies
- Configure security contexts and policies

### 4. Optimization
- Analyze resource utilization metrics
- Right-size resource requests and limits
- Optimize pod scheduling with affinity rules
- Tune autoscaling thresholds
- Implement cost optimization strategies

### 5. Security Hardening
- Implement least-privilege RBAC
- Configure network segmentation
- Enable pod security standards
- Implement secret management
- Configure audit logging

## Checklist

Before deploying to production:

- [ ] Resource requests and limits are properly configured
- [ ] Liveness and readiness probes are implemented
- [ ] Horizontal Pod Autoscaler (HPA) is configured if needed
- [ ] Pod Disruption Budget (PDB) ensures high availability
- [ ] RBAC policies follow least-privilege principle
- [ ] Network policies restrict unnecessary traffic
- [ ] Security context prevents privilege escalation
- [ ] Secrets are managed securely (not in plaintext)
- [ ] Labels and annotations follow organizational standards
- [ ] Monitoring and logging are configured
- [ ] Backup and disaster recovery procedures are documented
- [ ] Resource quotas prevent namespace overconsumption

## Usage Examples

### Example 1: Production-Ready Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-app
  namespace: production
  labels:
    app: web-app
    tier: frontend
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: web-app
  template:
    metadata:
      labels:
        app: web-app
    spec:
      serviceAccountName: web-app-sa
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 2000
      containers:
      - name: web
        image: myapp:v1.0.0
        ports:
        - containerPort: 8080
          name: http
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
        env:
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: password
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: app
                  operator: In
                  values:
                  - web-app
              topologyKey: kubernetes.io/hostname
```

### Example 2: Helm Chart with Values Hierarchy
```yaml
# Chart.yaml
apiVersion: v2
name: myapp
version: 1.0.0
appVersion: "1.0.0"
dependencies:
- name: postgresql
  version: 12.0.0
  repository: https://charts.bitnami.com/bitnami
  condition: postgresql.enabled

---
# values.yaml
replicaCount: 3

image:
  repository: myapp
  tag: v1.0.0
  pullPolicy: IfNotPresent

resources:
  requests:
    memory: 256Mi
    cpu: 100m
  limits:
    memory: 512Mi
    cpu: 500m

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70

postgresql:
  enabled: true
  auth:
    database: myapp
    username: myapp
```

### Example 3: HPA with Custom Metrics
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: web-app-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web-app
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  - type: Pods
    pods:
      metric:
        name: http_requests_per_second
      target:
        type: AverageValue
        averageValue: "1000"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Pods
        value: 1
        periodSeconds: 60
```

### Example 4: Network Policy for Namespace Isolation
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
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-web-to-db
  namespace: production
spec:
  podSelector:
    matchLabels:
      app: web-app
  policyTypes:
  - Egress
  egress:
  # Allow to database
  - to:
    - podSelector:
        matchLabels:
          app: postgresql
    ports:
    - protocol: TCP
      port: 5432
  # Allow DNS
  - to:
    - namespaceSelector:
        matchLabels:
          name: kube-system
      podSelector:
        matchLabels:
          k8s-app: kube-dns
    ports:
    - protocol: UDP
      port: 53
```

## Integration Points

### With Kafka Expert
- Deploy Kafka clusters with StatefulSets
- Configure persistent volume claims for broker storage
- Implement network policies for Kafka security
- Set up external access with LoadBalancer services

### With Database Optimizer
- Deploy database instances with StatefulSets
- Configure persistent storage and backup strategies
- Implement init containers for schema migrations
- Set up database connection pooling services

### With Airflow Architect
- Deploy Airflow with Helm charts (webserver, scheduler, workers)
- Configure persistent volumes for DAG storage
- Implement autoscaling for Celery workers
- Set up ingress for Airflow UI access

### With ETL Pipeline Builder
- Deploy ETL applications with proper resource allocation
- Configure job-based workloads with CronJobs
- Implement service mesh for microservices ETL
- Set up monitoring and logging for pipeline jobs

## References

See the `references/` directory for detailed guides on:
- **resource-patterns.md**: Resource request/limit calculations and QoS classes
- **networking-patterns.md**: Service types, ingress, and network policies
- **security-patterns.md**: RBAC, security contexts, and pod security standards
- **scaling-patterns.md**: HPA, VPA, cluster autoscaling, and PDB configuration
- **troubleshooting.md**: Common Kubernetes issues and debugging techniques

## Best Practices

1. **Always set resource requests** to enable proper scheduling
2. **Set resource limits** to prevent resource starvation
3. **Implement health probes** for self-healing capabilities
4. **Use namespaces** for logical isolation
5. **Apply labels consistently** for organization and selection
6. **Implement RBAC** following least-privilege principle
7. **Use ConfigMaps and Secrets** for configuration management
8. **Enable autoscaling** for dynamic workloads
9. **Configure PodDisruptionBudgets** for high availability
10. **Monitor resource utilization** and optimize continuously
