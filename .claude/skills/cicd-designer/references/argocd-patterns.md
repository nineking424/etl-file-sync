# ArgoCD GitOps 패턴

ArgoCD를 활용한 GitOps 기반 Kubernetes 배포 패턴입니다.

## 기본 Application

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: myapp
  namespace: argocd
  # Finalizer 설정으로 앱 삭제 시 리소스도 삭제
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: default

  source:
    repoURL: https://github.com/myorg/myapp-manifests
    targetRevision: HEAD
    path: k8s/overlays/production

  destination:
    server: https://kubernetes.default.svc
    namespace: production

  syncPolicy:
    automated:
      prune: true      # 삭제된 리소스 자동 제거
      selfHeal: true   # 드리프트 자동 수정
      allowEmpty: false
    syncOptions:
      - CreateNamespace=true
    retry:
      limit: 5
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 3m
```

## Helm Chart 배포

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: nginx-ingress
  namespace: argocd
spec:
  project: default

  source:
    chart: nginx-ingress
    repoURL: https://kubernetes.github.io/ingress-nginx
    targetRevision: 4.7.0
    helm:
      releaseName: nginx-ingress
      values: |
        controller:
          service:
            type: LoadBalancer
          metrics:
            enabled: true
      parameters:
        - name: controller.replicaCount
          value: "3"

  destination:
    server: https://kubernetes.default.svc
    namespace: ingress-nginx
```

## Kustomize 활용

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: myapp
  namespace: argocd
spec:
  source:
    repoURL: https://github.com/myorg/myapp
    targetRevision: HEAD
    path: k8s/overlays/production
    kustomize:
      namePrefix: prod-
      nameSuffix: -v2
      images:
        - myregistry.com/myapp:1.0.0=myregistry.com/myapp:2.0.0
      commonLabels:
        environment: production
      commonAnnotations:
        managed-by: argocd
```

## 멀티 소스 Application

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: myapp
spec:
  sources:
    - repoURL: https://github.com/myorg/myapp-config
      targetRevision: HEAD
      path: base

    - repoURL: https://github.com/myorg/myapp-secrets
      targetRevision: HEAD
      path: production
```

## Sync Waves (순차 배포)

```yaml
# 1. ConfigMap (wave 0)
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  annotations:
    argocd.argoproj.io/sync-wave: "0"
data:
  config.yaml: |
    server:
      port: 8080

---
# 2. Database (wave 1)
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  # ...

---
# 3. Application (wave 2)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
  annotations:
    argocd.argoproj.io/sync-wave: "2"
spec:
  # ...

---
# 4. Smoke Test (wave 3)
apiVersion: batch/v1
kind: Job
metadata:
  name: smoke-test
  annotations:
    argocd.argoproj.io/sync-wave: "3"
    argocd.argoproj.io/hook: PostSync
spec:
  # ...
```

## Hooks (Pre/Post Sync)

```yaml
# Pre-Sync Hook: 데이터베이스 마이그레이션
apiVersion: batch/v1
kind: Job
metadata:
  name: db-migration
  annotations:
    argocd.argoproj.io/hook: PreSync
    argocd.argoproj.io/hook-delete-policy: BeforeHookCreation
spec:
  template:
    spec:
      containers:
      - name: migration
        image: myapp:latest
        command: ["./migrate.sh"]
      restartPolicy: Never

---
# Post-Sync Hook: 배포 알림
apiVersion: batch/v1
kind: Job
metadata:
  name: deployment-notification
  annotations:
    argocd.argoproj.io/hook: PostSync
    argocd.argoproj.io/hook-delete-policy: HookSucceeded
spec:
  template:
    spec:
      containers:
      - name: notify
        image: curlimages/curl:latest
        command:
          - sh
          - -c
          - |
            curl -X POST https://hooks.slack.com/... \
              -d '{"text":"Deployment completed!"}'
      restartPolicy: Never
```

## ApplicationSet (멀티 환경)

### Git Generator

```yaml
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata:
  name: myapp-environments
  namespace: argocd
spec:
  generators:
  - git:
      repoURL: https://github.com/myorg/myapp-config
      revision: HEAD
      directories:
      - path: environments/*

  template:
    metadata:
      name: 'myapp-{{path.basename}}'
    spec:
      project: default
      source:
        repoURL: https://github.com/myorg/myapp-config
        targetRevision: HEAD
        path: '{{path}}'
      destination:
        server: https://kubernetes.default.svc
        namespace: '{{path.basename}}'
      syncPolicy:
        automated:
          prune: true
          selfHeal: true
```

### List Generator

```yaml
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata:
  name: myapp-clusters
spec:
  generators:
  - list:
      elements:
      - cluster: dev-cluster
        url: https://dev.k8s.example.com
        namespace: development
      - cluster: prod-cluster
        url: https://prod.k8s.example.com
        namespace: production

  template:
    metadata:
      name: 'myapp-{{cluster}}'
    spec:
      project: default
      source:
        repoURL: https://github.com/myorg/myapp
        targetRevision: HEAD
        path: k8s/{{namespace}}
      destination:
        server: '{{url}}'
        namespace: '{{namespace}}'
```

## Progressive Delivery (Argo Rollouts)

### Blue-Green 배포

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: myapp
spec:
  replicas: 3
  revisionHistoryLimit: 2
  selector:
    matchLabels:
      app: myapp

  template:
    metadata:
      labels:
        app: myapp
    spec:
      containers:
      - name: myapp
        image: myregistry.com/myapp:latest

  strategy:
    blueGreen:
      activeService: myapp-active
      previewService: myapp-preview
      autoPromotionEnabled: false
      scaleDownDelaySeconds: 30
      prePromotionAnalysis:
        templates:
        - templateName: smoke-test
      postPromotionAnalysis:
        templates:
        - templateName: health-check
```

### Canary 배포

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: myapp
spec:
  replicas: 5
  strategy:
    canary:
      canaryService: myapp-canary
      stableService: myapp-stable
      steps:
      - setWeight: 20
      - pause: {duration: 2m}
      - setWeight: 40
      - pause: {duration: 2m}
      - analysis:
          templates:
          - templateName: success-rate
          - templateName: error-rate
          args:
          - name: service-name
            value: myapp-canary
      - setWeight: 60
      - pause: {duration: 2m}
      - setWeight: 80
      - pause: {duration: 2m}

      # Istio를 통한 트래픽 분할
      trafficRouting:
        istio:
          virtualService:
            name: myapp-vsvc
            routes:
            - primary
```

### Analysis Template

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: success-rate
spec:
  args:
  - name: service-name

  metrics:
  - name: success-rate
    interval: 1m
    count: 5
    successCondition: result >= 0.95
    failureLimit: 3
    provider:
      prometheus:
        address: http://prometheus:9090
        query: |
          sum(rate(
            http_requests_total{
              service="{{args.service-name}}",
              status!~"5.."
            }[5m]
          )) /
          sum(rate(
            http_requests_total{
              service="{{args.service-name}}"
            }[5m]
          ))

  - name: latency
    interval: 1m
    count: 5
    successCondition: result < 500
    provider:
      prometheus:
        address: http://prometheus:9090
        query: |
          histogram_quantile(0.95,
            rate(http_request_duration_milliseconds_bucket{
              service="{{args.service-name}}"
            }[5m])
          )
```

## AppProject (프로젝트 격리)

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AppProject
metadata:
  name: production
  namespace: argocd
spec:
  description: Production applications

  # 허용된 소스 리포지토리
  sourceRepos:
  - 'https://github.com/myorg/*'

  # 허용된 배포 대상
  destinations:
  - namespace: 'production'
    server: https://kubernetes.default.svc
  - namespace: 'prod-*'
    server: https://kubernetes.default.svc

  # 허용된 클러스터 리소스
  clusterResourceWhitelist:
  - group: ''
    kind: Namespace

  # 네임스페이스 리소스 화이트리스트
  namespaceResourceWhitelist:
  - group: 'apps'
    kind: Deployment
  - group: 'apps'
    kind: StatefulSet
  - group: ''
    kind: Service

  # 네임스페이스 리소스 블랙리스트
  namespaceResourceBlacklist:
  - group: ''
    kind: ResourceQuota

  # RBAC 정책
  roles:
  - name: developer
    description: Read-only access
    policies:
    - p, proj:production:developer, applications, get, production/*, allow
    - p, proj:production:developer, applications, sync, production/*, deny

  - name: admin
    description: Full access
    policies:
    - p, proj:production:admin, applications, *, production/*, allow
```

## Notification (알림 설정)

```yaml
# argocd-notifications-cm ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: argocd-notifications-cm
data:
  service.slack: |
    token: $slack-token

  template.app-deployed: |
    message: |
      Application {{.app.metadata.name}} has been deployed!
      Revision: {{.app.status.sync.revision}}
    slack:
      attachments: |
        [{
          "title": "{{.app.metadata.name}}",
          "color": "good",
          "fields": [{
            "title": "Sync Status",
            "value": "{{.app.status.sync.status}}",
            "short": true
          }, {
            "title": "Revision",
            "value": "{{.app.status.sync.revision}}",
            "short": true
          }]
        }]

  trigger.on-deployed: |
    - when: app.status.operationState.phase in ['Succeeded']
      send: [app-deployed]

---
# Application에 알림 구독
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: myapp
  annotations:
    notifications.argoproj.io/subscribe.on-deployed.slack: my-channel
```

## 체크리스트

- [ ] Application 매니페스트 작성
- [ ] Sync Policy 설정
- [ ] Health Check 구성
- [ ] Sync Waves로 순차 배포
- [ ] Hooks 활용 (마이그레이션, 테스트)
- [ ] ApplicationSet으로 멀티 환경 관리
- [ ] Progressive Delivery 전략 선택
- [ ] Analysis Template 정의
- [ ] AppProject로 격리
- [ ] Notification 설정
