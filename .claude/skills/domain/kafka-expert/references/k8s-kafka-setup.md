# Kubernetes Kafka Setup

Comprehensive guide to deploying and managing Apache Kafka on Kubernetes with external access and production-ready configurations.

## StatefulSet Configuration

### Basic Kafka StatefulSet
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
  namespace: kafka
spec:
  serviceName: kafka-headless
  replicas: 3
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
      - name: kafka
        image: confluentinc/cp-kafka:7.5.0
        ports:
        - containerPort: 9092
          name: internal
        - containerPort: 9093
          name: controller
        - containerPort: 9094
          name: external
        env:
        - name: KAFKA_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: KAFKA_PROCESS_ROLES
          value: "broker,controller"
        - name: KAFKA_LISTENERS
          value: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094"
        volumeMounts:
        - name: data
          mountPath: /var/lib/kafka/data
        resources:
          requests:
            memory: "4Gi"
            cpu: "2000m"
          limits:
            memory: "8Gi"
            cpu: "4000m"
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 100Gi
```

### Headless Service
```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka-headless
  namespace: kafka
spec:
  clusterIP: None
  selector:
    app: kafka
  ports:
  - port: 9092
    name: internal
  - port: 9093
    name: controller
```

## External Access with LoadBalancer

### Per-Broker LoadBalancer Services
```yaml
# Service for kafka-0
apiVersion: v1
kind: Service
metadata:
  name: kafka-0-external
  namespace: kafka
spec:
  type: LoadBalancer
  selector:
    app: kafka
    statefulset.kubernetes.io/pod-name: kafka-0
  ports:
  - port: 9094
    targetPort: 9094
    protocol: TCP
    name: external
---
# Service for kafka-1
apiVersion: v1
kind: Service
metadata:
  name: kafka-1-external
  namespace: kafka
spec:
  type: LoadBalancer
  selector:
    app: kafka
    statefulset.kubernetes.io/pod-name: kafka-1
  ports:
  - port: 9094
    targetPort: 9094
    protocol: TCP
    name: external
---
# Service for kafka-2
apiVersion: v1
kind: Service
metadata:
  name: kafka-2-external
  namespace: kafka
spec:
  type: LoadBalancer
  selector:
    app: kafka
    statefulset.kubernetes.io/pod-name: kafka-2
  ports:
  - port: 9094
    targetPort: 9094
    protocol: TCP
    name: external
```

### Advertised Listeners Configuration
```yaml
# ConfigMap for broker configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-config
  namespace: kafka
data:
  server.properties: |
    # Listeners
    listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094

    # Advertised listeners - will be generated per pod
    advertised.listeners=PLAINTEXT://kafka-0.kafka-headless:9092,EXTERNAL://${EXTERNAL_HOSTNAME}:9094

    # Listener security protocol map
    listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT

    # Inter-broker communication
    inter.broker.listener.name=PLAINTEXT

    # Controller settings
    controller.listener.names=CONTROLLER
```

### Init Container for Dynamic Configuration
```yaml
initContainers:
- name: init-config
  image: busybox
  command:
  - sh
  - -c
  - |
    POD_NAME=${HOSTNAME}
    BROKER_ID=${POD_NAME##*-}

    # Get external IP from service
    EXTERNAL_IP=$(nslookup kafka-${BROKER_ID}-external | grep Address | tail -1 | awk '{print $2}')

    # Generate advertised listeners
    cat > /config/server.properties <<EOF
    advertised.listeners=PLAINTEXT://${POD_NAME}.kafka-headless:9092,EXTERNAL://${EXTERNAL_IP}:9094
    EOF
  volumeMounts:
  - name: config
    mountPath: /config
```

## KRaft Mode Configuration

### KRaft StatefulSet
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  serviceName: kafka-headless
  replicas: 3
  template:
    spec:
      containers:
      - name: kafka
        image: confluentinc/cp-kafka:7.5.0
        env:
        # KRaft mode settings
        - name: KAFKA_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.labels['statefulset.kubernetes.io/pod-name']
        - name: KAFKA_PROCESS_ROLES
          value: "broker,controller"
        - name: KAFKA_CONTROLLER_QUORUM_VOTERS
          value: "0@kafka-0.kafka-headless:9093,1@kafka-1.kafka-headless:9093,2@kafka-2.kafka-headless:9093"
        - name: KAFKA_CONTROLLER_LISTENER_NAMES
          value: "CONTROLLER"
        - name: KAFKA_METADATA_LOG_DIR
          value: "/var/lib/kafka/metadata"

        # Listener configuration
        - name: KAFKA_LISTENERS
          value: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094"
        - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
          value: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT"
        - name: KAFKA_INTER_BROKER_LISTENER_NAME
          value: "PLAINTEXT"

        # Log settings
        - name: KAFKA_LOG_DIRS
          value: "/var/lib/kafka/data"

        # Cluster settings
        - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
          value: "3"
        - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
          value: "3"
        - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
          value: "2"
```

### Format Storage (One-time Setup)
```bash
# Format storage before first start
kubectl exec -it kafka-0 -- kafka-storage.sh format \
  -t $(kafka-storage.sh random-uuid) \
  -c /etc/kafka/server.properties
```

## Resource Management

### Resource Requests and Limits
```yaml
resources:
  requests:
    memory: "4Gi"
    cpu: "2000m"
    ephemeral-storage: "10Gi"
  limits:
    memory: "8Gi"
    cpu: "4000m"
    ephemeral-storage: "20Gi"

# Considerations:
# - Memory: min 4GB for production, scale with throughput
# - CPU: 2-4 cores typical, more for high throughput
# - Storage: Fast SSD required for good performance
```

### JVM Configuration
```yaml
env:
- name: KAFKA_HEAP_OPTS
  value: "-Xms4g -Xmx4g"
- name: KAFKA_JVM_PERFORMANCE_OPTS
  value: "-XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M"

# Heap size guidelines:
# - Set Xms = Xmx (avoid dynamic resizing)
# - Use 50% of container memory for heap
# - Reserve memory for OS page cache
# - Monitor GC metrics and adjust
```

### Storage Classes
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: kafka-storage
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp3
  iops: "16000"
  throughput: "1000"
  fsType: xfs
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

## Network Policies

### Kafka Network Policy
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kafka-network-policy
  namespace: kafka
spec:
  podSelector:
    matchLabels:
      app: kafka
  policyTypes:
  - Ingress
  - Egress
  ingress:
  # Allow internal broker communication
  - from:
    - podSelector:
        matchLabels:
          app: kafka
    ports:
    - protocol: TCP
      port: 9092
    - protocol: TCP
      port: 9093
  # Allow from application namespace
  - from:
    - namespaceSelector:
        matchLabels:
          name: applications
    ports:
    - protocol: TCP
      port: 9092
  egress:
  # Allow to other brokers
  - to:
    - podSelector:
        matchLabels:
          app: kafka
    ports:
    - protocol: TCP
      port: 9092
    - protocol: TCP
      port: 9093
  # Allow DNS
  - to:
    - namespaceSelector: {}
      podSelector:
        matchLabels:
          k8s-app: kube-dns
    ports:
    - protocol: UDP
      port: 53
```

## Monitoring and Observability

### Prometheus Monitoring
```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka-metrics
  namespace: kafka
  labels:
    app: kafka
spec:
  ports:
  - port: 7071
    name: metrics
  selector:
    app: kafka

---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: kafka-metrics
  namespace: kafka
spec:
  selector:
    matchLabels:
      app: kafka
  endpoints:
  - port: metrics
    interval: 30s
```

### JMX Exporter Configuration
```yaml
env:
- name: KAFKA_JMX_OPTS
  value: "-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx_exporter/kafka-config.yml"

# Key metrics to monitor:
# - kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec
# - kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec
# - kafka.controller:type=KafkaController,name=ActiveControllerCount
# - kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
# - kafka.server:type=ReplicaManager,name=PartitionCount
```

## High Availability

### Pod Disruption Budget
```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: kafka-pdb
  namespace: kafka
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: kafka
```

### Anti-Affinity Rules
```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchExpressions:
        - key: app
          operator: In
          values:
          - kafka
      topologyKey: kubernetes.io/hostname
  # Prefer different availability zones
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchExpressions:
          - key: app
            operator: In
            values:
            - kafka
        topologyKey: topology.kubernetes.io/zone
```

## Disaster Recovery

### Backup Configuration
```yaml
# Velero backup schedule
apiVersion: velero.io/v1
kind: Schedule
metadata:
  name: kafka-backup
  namespace: velero
spec:
  schedule: "0 2 * * *"
  template:
    includedNamespaces:
    - kafka
    includedResources:
    - persistentvolumeclaims
    - persistentvolumes
    ttl: 720h
```

### Recovery Procedure
```bash
# 1. Stop Kafka StatefulSet
kubectl scale statefulset kafka --replicas=0 -n kafka

# 2. Restore PVCs from backup
velero restore create --from-backup kafka-backup-20240101

# 3. Start Kafka StatefulSet
kubectl scale statefulset kafka --replicas=3 -n kafka

# 4. Verify cluster health
kubectl exec kafka-0 -- kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

## Best Practices

1. **Use StatefulSets** for stable network identities
2. **Enable pod anti-affinity** to spread brokers across nodes
3. **Use fast SSD storage** with high IOPS
4. **Configure resource limits** to prevent noisy neighbors
5. **Implement network policies** for security
6. **Monitor JVM metrics** and tune GC settings
7. **Use PodDisruptionBudgets** to maintain availability
8. **Regular backups** of metadata and configuration
9. **Test failover scenarios** in staging environment
10. **Document external DNS** mapping for client access
