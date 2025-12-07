# Resource Patterns

Comprehensive guide to Kubernetes resource requests, limits, and Quality of Service (QoS) classes.

## Resource Request and Limit Basics

### CPU Resources
```yaml
resources:
  requests:
    cpu: "100m"  # 0.1 CPU core (100 millicores)
  limits:
    cpu: "500m"  # 0.5 CPU core maximum

# CPU units:
# - 1 CPU = 1000m (millicores)
# - 100m = 0.1 CPU = 10% of one core
# - Can request fractional CPUs
# - Compressible resource (throttled when exceeded)
```

### Memory Resources
```yaml
resources:
  requests:
    memory: "256Mi"  # 256 Mebibytes
  limits:
    memory: "512Mi"  # 512 Mebibytes maximum

# Memory units:
# - Ki, Mi, Gi, Ti (binary: 1024-based)
# - K, M, G, T (decimal: 1000-based)
# - Non-compressible resource (OOM kill when exceeded)
```

### Ephemeral Storage
```yaml
resources:
  requests:
    ephemeral-storage: "1Gi"
  limits:
    ephemeral-storage: "2Gi"

# For temporary files, logs, EmptyDir volumes
# Pod evicted when limit exceeded
```

## Quality of Service Classes

### Guaranteed QoS
```yaml
# Highest priority - last to be evicted
# Requirements:
# - All containers have requests = limits
# - CPU and memory both specified

spec:
  containers:
  - name: app
    resources:
      requests:
        memory: "256Mi"
        cpu: "500m"
      limits:
        memory: "256Mi"  # Same as request
        cpu: "500m"      # Same as request

# Use for: Critical applications, databases, stateful services
```

### Burstable QoS
```yaml
# Medium priority
# Requirements:
# - At least one container has requests or limits
# - Requests != limits (can burst)

spec:
  containers:
  - name: app
    resources:
      requests:
        memory: "128Mi"
        cpu: "100m"
      limits:
        memory: "512Mi"  # Can burst to 512Mi
        cpu: "1000m"     # Can burst to 1 CPU

# Use for: Most applications, web servers, workers
```

### BestEffort QoS
```yaml
# Lowest priority - first to be evicted
# Requirements:
# - No requests or limits specified

spec:
  containers:
  - name: app
    # No resources specified

# Use for: Batch jobs, non-critical tasks, dev environments
# Avoid in production for important workloads
```

## Resource Calculation Strategies

### Application Profiling Method
```bash
# 1. Run application under load
kubectl run test-pod --image=myapp --requests='cpu=0,memory=0'

# 2. Monitor resource usage
kubectl top pod test-pod
# NAME       CPU(cores)   MEMORY(bytes)
# test-pod   150m         400Mi

# 3. Calculate requests (add 20-30% buffer)
requests:
  cpu: 200m      # 150m * 1.3
  memory: 520Mi  # 400Mi * 1.3

# 4. Set limits (2-3x requests for burstability)
limits:
  cpu: 500m      # 200m * 2.5
  memory: 1Gi    # 520Mi * 2
```

### Historical Data Method
```python
# Use Prometheus to analyze historical usage
# Query average and peak usage over 30 days

# PromQL queries
avg_cpu = avg_over_time(container_cpu_usage_seconds_total[30d])
p95_cpu = quantile_over_time(0.95, container_cpu_usage_seconds_total[30d])

avg_memory = avg_over_time(container_memory_usage_bytes[30d])
p95_memory = quantile_over_time(0.95, container_memory_usage_bytes[30d])

# Set requests to average + buffer
# Set limits to P95 + buffer
```

### Right-Sizing Formula
```yaml
# Conservative approach
requests:
  cpu: avg_cpu * 1.2
  memory: avg_memory * 1.2

limits:
  cpu: p95_cpu * 1.3
  memory: p95_memory * 1.3

# Aggressive approach (better bin packing)
requests:
  cpu: avg_cpu * 1.1
  memory: avg_memory * 1.1

limits:
  cpu: p95_cpu * 1.2
  memory: p95_memory * 1.2
```

## Resource Patterns by Workload Type

### Web Application
```yaml
# Typical web server
resources:
  requests:
    memory: "256Mi"
    cpu: "100m"
  limits:
    memory: "512Mi"
    cpu: "500m"

# Characteristics:
# - Variable CPU usage (bursty)
# - Steady memory usage
# - Can tolerate CPU throttling
# - Burstable QoS appropriate
```

### Database
```yaml
# Production database
resources:
  requests:
    memory: "2Gi"
    cpu: "1000m"
  limits:
    memory: "2Gi"
    cpu: "1000m"

# Characteristics:
# - Guaranteed QoS required
# - Predictable resource usage
# - Cannot tolerate throttling
# - Memory-intensive
```

### Batch Job
```yaml
# Data processing job
resources:
  requests:
    memory: "512Mi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "2000m"

# Characteristics:
# - High burstability needed
# - Can tolerate throttling
# - Time-insensitive
# - Variable resource usage
```

### Cache (Redis/Memcached)
```yaml
# In-memory cache
resources:
  requests:
    memory: "4Gi"
    cpu: "500m"
  limits:
    memory: "4Gi"
    cpu: "1000m"

# Characteristics:
# - Memory is critical (Guaranteed)
# - CPU can burst (Burstable)
# - OOM kill would be catastrophic
# - High memory, low CPU
```

### Queue Worker
```yaml
# Background worker
resources:
  requests:
    memory: "256Mi"
    cpu: "200m"
  limits:
    memory: "1Gi"
    cpu: "1000m"

# Characteristics:
# - Bursty workload
# - Can scale horizontally
# - Tolerates throttling
# - Memory varies with job size
```

## Resource Quotas

### Namespace Resource Quota
```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: compute-quota
  namespace: production
spec:
  hard:
    requests.cpu: "100"
    requests.memory: "200Gi"
    limits.cpu: "200"
    limits.memory: "400Gi"
    pods: "50"
    services: "10"
    persistentvolumeclaims: "20"

# Prevents namespace from consuming all cluster resources
# Requires all pods to specify requests/limits
```

### Limit Ranges
```yaml
apiVersion: v1
kind: LimitRange
metadata:
  name: resource-limits
  namespace: production
spec:
  limits:
  # Container defaults
  - type: Container
    default:
      cpu: "500m"
      memory: "512Mi"
    defaultRequest:
      cpu: "100m"
      memory: "256Mi"
    max:
      cpu: "4000m"
      memory: "8Gi"
    min:
      cpu: "50m"
      memory: "64Mi"

  # Pod limits
  - type: Pod
    max:
      cpu: "8000m"
      memory: "16Gi"
    min:
      cpu: "100m"
      memory: "128Mi"

  # PVC limits
  - type: PersistentVolumeClaim
    max:
      storage: "100Gi"
    min:
      storage: "1Gi"
```

## Advanced Resource Patterns

### Init Container Resources
```yaml
spec:
  initContainers:
  - name: init-db
    image: postgres-client
    command: ['sh', '-c', 'until pg_isready; do sleep 1; done']
    resources:
      requests:
        cpu: "50m"
        memory: "64Mi"
      limits:
        cpu: "100m"
        memory: "128Mi"

  # Init containers run sequentially before app containers
  # Resources are released after completion
  # Don't over-allocate resources for short-lived init containers
```

### Sidecar Resource Allocation
```yaml
spec:
  containers:
  # Main application
  - name: app
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
      limits:
        cpu: "1000m"
        memory: "1Gi"

  # Logging sidecar
  - name: log-forwarder
    resources:
      requests:
        cpu: "50m"
        memory: "64Mi"
      limits:
        cpu: "200m"
        memory: "256Mi"

  # Total pod resources = sum of all containers
  # requests: 550m CPU, 576Mi memory
  # limits: 1200m CPU, 1.25Gi memory
```

### Extended Resources
```yaml
# GPU resources
resources:
  requests:
    nvidia.com/gpu: 1
  limits:
    nvidia.com/gpu: 1

# Custom resources registered with kubelet
# Examples: GPUs, FPGAs, network bandwidth
# Always integer values
```

## Monitoring and Optimization

### VPA (Vertical Pod Autoscaler)
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: web-app-vpa
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web-app
  updatePolicy:
    updateMode: "Auto"  # Auto, Recreate, Initial, Off
  resourcePolicy:
    containerPolicies:
    - containerName: app
      minAllowed:
        cpu: "100m"
        memory: "128Mi"
      maxAllowed:
        cpu: "2000m"
        memory: "4Gi"
      controlledResources: ["cpu", "memory"]

# VPA automatically adjusts requests/limits based on usage
# Useful for right-sizing workloads
```

### Resource Utilization Metrics
```bash
# Check pod resource usage
kubectl top pods -n production

# Check node resource usage
kubectl top nodes

# Detailed metrics via Prometheus
container_cpu_usage_seconds_total
container_memory_usage_bytes
container_memory_working_set_bytes
```

## Cost Optimization

### Bin Packing Strategy
```yaml
# Use smaller, more accurate requests
# Allows better node utilization

# Before: Wasteful
requests:
  cpu: "1000m"
  memory: "2Gi"
# Actual usage: 200m CPU, 512Mi memory
# Waste: 80% CPU, 75% memory

# After: Optimized
requests:
  cpu: "250m"
  memory: "640Mi"
# Actual usage: 200m CPU, 512Mi memory
# Waste: 20% CPU, 20% memory
```

### Resource Overcommitment
```yaml
# Allow more pods per node
# Set limits higher than requests

resources:
  requests:
    cpu: "100m"    # Guaranteed allocation
    memory: "256Mi"
  limits:
    cpu: "500m"    # Can burst to 5x
    memory: "1Gi"  # Can burst to 4x

# Trade-off: Higher density vs potential throttling
# Monitor for throttling and OOM kills
```

## Best Practices

1. **Always set requests** for proper scheduling
2. **Set limits** to prevent resource starvation
3. **Use Guaranteed QoS** for critical workloads
4. **Profile applications** before setting resources
5. **Monitor actual usage** vs requests/limits
6. **Use VPA** for automatic right-sizing
7. **Implement resource quotas** per namespace
8. **Review and optimize** regularly (monthly)
9. **Document resource rationale** for team knowledge
10. **Test under load** before production deployment
