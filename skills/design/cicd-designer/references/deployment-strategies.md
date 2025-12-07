# 배포 전략

다양한 배포 전략의 특징, 장단점, 구현 방법을 설명합니다.

## 1. Rolling Deployment (롤링 배포)

### 개념
기존 버전을 점진적으로 새 버전으로 교체합니다.

### 특징
- 무중단 배포
- 리소스 효율적
- 간단한 구현

### Kubernetes 구현

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
spec:
  replicas: 10
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 2        # 최대 2개 추가 파드
      maxUnavailable: 1  # 최대 1개 사용 불가
  template:
    spec:
      containers:
      - name: myapp
        image: myapp:v2
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

### 장점
- 리소스 사용 효율적
- 간단한 설정
- 점진적 배포

### 단점
- 두 버전 동시 실행
- 롤백 시간 소요
- 버전 간 호환성 필요

## 2. Blue-Green Deployment

### 개념
두 개의 동일한 환경(Blue/Green)을 유지하고 트래픽을 전환합니다.

### Kubernetes + Service 구현

```yaml
# Blue Deployment (현재 프로덕션)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp-blue
  labels:
    version: blue
spec:
  replicas: 3
  selector:
    matchLabels:
      app: myapp
      version: blue
  template:
    metadata:
      labels:
        app: myapp
        version: blue
    spec:
      containers:
      - name: myapp
        image: myapp:1.0.0

---
# Green Deployment (새 버전)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp-green
  labels:
    version: green
spec:
  replicas: 3
  selector:
    matchLabels:
      app: myapp
      version: green
  template:
    metadata:
      labels:
        app: myapp
        version: green
    spec:
      containers:
      - name: myapp
        image: myapp:2.0.0

---
# Service (트래픽 전환)
apiVersion: v1
kind: Service
metadata:
  name: myapp
spec:
  selector:
    app: myapp
    version: blue  # green으로 변경하여 트래픽 전환
  ports:
  - port: 80
    targetPort: 8080
```

### Istio를 활용한 구현

```yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: myapp
spec:
  hosts:
  - myapp.example.com
  http:
  - match:
    - headers:
        x-version:
          exact: green
    route:
    - destination:
        host: myapp-green
  - route:
    - destination:
        host: myapp-blue
      weight: 100
    - destination:
        host: myapp-green
      weight: 0
```

### 장점
- 즉시 롤백 가능
- 철저한 테스트 가능
- 명확한 버전 분리

### 단점
- 2배의 리소스 필요
- 데이터베이스 마이그레이션 복잡
- 비용 증가

## 3. Canary Deployment (카나리 배포)

### 개념
신규 버전을 소수 사용자에게 먼저 배포하고 점진적으로 확대합니다.

### Argo Rollouts 구현

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: myapp
spec:
  replicas: 10
  strategy:
    canary:
      steps:
      - setWeight: 10   # 10% 트래픽
      - pause:
          duration: 5m
      - setWeight: 25   # 25% 트래픽
      - pause:
          duration: 5m
      - setWeight: 50   # 50% 트래픽
      - pause:
          duration: 5m
      - setWeight: 75   # 75% 트래픽
      - pause:
          duration: 5m

      # Analysis (자동 판단)
      analysis:
        templates:
        - templateName: success-rate
        startingStep: 2
        args:
        - name: service-name
          value: myapp-canary

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
        image: myapp:2.0.0
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
    successCondition: result >= 0.95
    failureLimit: 3
    provider:
      prometheus:
        address: http://prometheus:9090
        query: |
          sum(rate(http_requests_total{
            service="{{args.service-name}}",
            status!~"5.."
          }[5m])) /
          sum(rate(http_requests_total{
            service="{{args.service-name}}"
          }[5m]))

  - name: error-rate
    interval: 1m
    successCondition: result < 0.05
    provider:
      prometheus:
        address: http://prometheus:9090
        query: |
          sum(rate(http_requests_total{
            service="{{args.service-name}}",
            status=~"5.."
          }[5m])) /
          sum(rate(http_requests_total{
            service="{{args.service-name}}"
          }[5m]))
```

### 장점
- 위험 최소화
- 점진적 확대
- 실시간 피드백
- 자동 롤백 가능

### 단점
- 복잡한 구성
- 모니터링 필수
- 긴 배포 시간

## 4. A/B Testing

### 개념
특정 사용자 그룹에 다른 버전을 제공하여 테스트합니다.

### Istio 구현

```yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: myapp
spec:
  hosts:
  - myapp.example.com
  http:
  # 지역 기반 라우팅
  - match:
    - headers:
        x-user-location:
          exact: "US"
    route:
    - destination:
        host: myapp
        subset: v2
      weight: 50
    - destination:
        host: myapp
        subset: v1
      weight: 50

  # 나머지 트래픽
  - route:
    - destination:
        host: myapp
        subset: v1

---
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: myapp
spec:
  host: myapp
  subsets:
  - name: v1
    labels:
      version: v1
  - name: v2
    labels:
      version: v2
```

### 장점
- 실제 사용자 피드백
- 데이터 기반 의사결정
- 점진적 롤아웃

### 단점
- 복잡한 구현
- 분석 도구 필요
- 버전 관리 복잡

## 5. Shadow Deployment (미러링)

### 개념
프로덕션 트래픽을 복사하여 새 버전으로 전송하지만 응답은 무시합니다.

### Istio 구현

```yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: myapp
spec:
  hosts:
  - myapp
  http:
  - route:
    - destination:
        host: myapp
        subset: v1
      weight: 100
    mirror:
      host: myapp
      subset: v2
    mirrorPercentage:
      value: 100  # 100% 트래픽 미러링
```

### 장점
- 프로덕션 환경에서 안전한 테스트
- 성능 테스트 가능
- 사용자 영향 없음

### 단점
- 추가 리소스 필요
- 부작용 처리 필요
- 완전한 검증 어려움

## 6. Feature Toggle (기능 플래그)

### 개념
코드 내에서 기능을 켜고 끄는 방식으로 배포합니다.

### 구현 예제

```javascript
// LaunchDarkly, Unleash 등 사용
const FeatureToggle = require('feature-toggle');

app.get('/api/products', async (req, res) => {
  const useNewAlgorithm = await FeatureToggle.isEnabled(
    'new-recommendation-algorithm',
    { userId: req.user.id }
  );

  let products;
  if (useNewAlgorithm) {
    products = await getProductsV2();
  } else {
    products = await getProductsV1();
  }

  res.json(products);
});
```

### ConfigMap 기반 구현

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: feature-flags
data:
  features.json: |
    {
      "new-ui": {
        "enabled": true,
        "rollout": 50
      },
      "new-checkout": {
        "enabled": false
      }
    }
```

### 장점
- 빠른 롤백
- 점진적 롤아웃
- A/B 테스트 용이

### 단점
- 코드 복잡도 증가
- 기술 부채 발생 가능
- 테스트 복잡

## 배포 전략 비교

| 전략 | 롤백 속도 | 리소스 사용 | 복잡도 | 위험도 |
|------|----------|------------|--------|--------|
| Rolling | 중간 | 낮음 | 낮음 | 중간 |
| Blue-Green | 즉시 | 높음 | 중간 | 낮음 |
| Canary | 빠름 | 중간 | 높음 | 낮음 |
| A/B Testing | 빠름 | 중간 | 높음 | 낮음 |
| Shadow | N/A | 높음 | 높음 | 매우 낮음 |
| Feature Toggle | 즉시 | 낮음 | 중간 | 낮음 |

## 선택 가이드

### Rolling Deployment 선택 시
- 간단한 애플리케이션
- 리소스 제약
- 하위 호환성 보장

### Blue-Green 선택 시
- 즉시 롤백 필요
- 충분한 리소스
- 철저한 테스트 필요

### Canary 선택 시
- 위험 최소화 중요
- 모니터링 인프라 구축됨
- 점진적 롤아웃 필요

### A/B Testing 선택 시
- 사용자 피드백 수집
- 기능 검증
- 데이터 기반 의사결정

### Shadow 선택 시
- 프로덕션 트래픽으로 테스트
- 성능 검증
- 안전한 테스트 환경

### Feature Toggle 선택 시
- 빠른 기능 on/off 필요
- 점진적 기능 출시
- 긴급 롤백 대비

## 체크리스트

- [ ] 배포 전략 선택
- [ ] Health Check 엔드포인트 구현
- [ ] Readiness/Liveness Probe 설정
- [ ] 모니터링 메트릭 정의
- [ ] 자동 롤백 조건 설정
- [ ] Smoke Test 작성
- [ ] 알림 설정
- [ ] 롤백 프로시저 문서화
- [ ] 배포 스크립트 자동화
- [ ] 배포 후 검증 절차 수립
