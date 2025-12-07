# Scaling Patterns

Guide to HPA, VPA, cluster autoscaling, and pod disruption budgets.

## Horizontal Pod Autoscaler (HPA)

### CPU-Based HPA
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: web-hpa
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
```

### Memory-Based HPA
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: memory-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: memory-app
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

### Custom Metrics HPA
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: custom-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web-app
  minReplicas: 3
  maxReplicas: 50
  metrics:
  - type: Pods
    pods:
      metric:
        name: http_requests_per_second
      target:
        type: AverageValue
        averageValue: "1000"
  - type: External
    external:
      metric:
        name: queue_length
        selector:
          matchLabels:
            queue: tasks
      target:
        type: Value
        value: "100"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
      - type: Pods
        value: 5
        periodSeconds: 60
      selectPolicy: Max
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Pods
        value: 1
        periodSeconds: 60
      selectPolicy: Min
```

## Vertical Pod Autoscaler (VPA)

### Basic VPA
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: web-vpa
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web-app
  updatePolicy:
    updateMode: "Auto"
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
      mode: Auto
```

### VPA Recommendation Only
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: web-vpa-recommender
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web-app
  updatePolicy:
    updateMode: "Off"  # Recommendation only
```

## Pod Disruption Budget

### Minimum Available
```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: web-pdb
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: web-app
```

### Maximum Unavailable
```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: web-pdb-max
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app: web-app
```

### Percentage-Based
```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: web-pdb-percent
spec:
  minAvailable: 75%
  selector:
    matchLabels:
      app: web-app
```

## Cluster Autoscaler

### Node Pool Configuration
```yaml
# GKE example
gcloud container node-pools create workers \
  --cluster=my-cluster \
  --enable-autoscaling \
  --min-nodes=3 \
  --max-nodes=20 \
  --machine-type=n1-standard-4
```

### Cluster Autoscaler Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cluster-autoscaler
  namespace: kube-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cluster-autoscaler
  template:
    metadata:
      labels:
        app: cluster-autoscaler
    spec:
      serviceAccountName: cluster-autoscaler
      containers:
      - name: cluster-autoscaler
        image: k8s.gcr.io/autoscaling/cluster-autoscaler:v1.27.0
        command:
        - ./cluster-autoscaler
        - --v=4
        - --cloud-provider=gce
        - --skip-nodes-with-local-storage=false
        - --expander=least-waste
        - --scale-down-enabled=true
        - --scale-down-delay-after-add=10m
        - --scale-down-unneeded-time=10m
```

## KEDA (Event-Driven Autoscaling)

### Kafka Scaler
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: kafka-scaler
spec:
  scaleTargetRef:
    name: kafka-consumer
  pollingInterval: 30
  cooldownPeriod: 300
  minReplicaCount: 1
  maxReplicaCount: 50
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: kafka:9092
      consumerGroup: my-group
      topic: events
      lagThreshold: "1000"
```

### Prometheus Scaler
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: prometheus-scaler
spec:
  scaleTargetRef:
    name: web-app
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus:9090
      metricName: http_requests_total
      query: sum(rate(http_requests_total[2m]))
      threshold: "100"
```

## Best Practices

1. **Set appropriate min/max replicas** for HPA
2. **Use stabilization windows** to prevent flapping
3. **Combine HPA and VPA carefully** (not recommended together)
4. **Always configure PDB** for high availability
5. **Monitor autoscaling events** for tuning
6. **Set resource requests** for HPA to work
7. **Use custom metrics** for business-driven scaling
8. **Test autoscaling** under realistic load
9. **Configure proper cooldown periods**
10. **Monitor cluster autoscaler** logs for issues
