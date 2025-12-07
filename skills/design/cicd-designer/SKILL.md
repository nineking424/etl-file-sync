---
name: cicd-designer
description: Design and implement CI/CD pipelines for GitHub Actions, Jenkins, and ArgoCD with deployment strategies including Blue-Green and Canary
---

# CI/CD Designer Skill

GitHub Actions, Jenkins, ArgoCD를 위한 CI/CD 파이프라인 설계 및 구현, Blue-Green 및 Canary를 포함한 배포 전략을 지원합니다.

## Overview

CI/CD Designer는 현대적인 CI/CD 파이프라인을 설계하고 구현하는 전문 스킬입니다. GitHub Actions, Jenkins, ArgoCD 같은 주요 CI/CD 도구에 대한 파이프라인 생성, Blue-Green 및 Canary 배포 전략 구현, 보안 스캔 통합, 자동화된 테스트 및 배포를 포함한 포괄적인 CI/CD 솔루션을 제공합니다.

## Core Capabilities

### 1. GitHub Actions 워크플로우 생성
- 빌드 및 테스트 자동화
- 다중 환경 배포 (dev, staging, prod)
- Docker 이미지 빌드 및 푸시
- 시맨틱 버저닝 자동화
- 의존성 캐싱
- Matrix 빌드 (다중 언어/버전)

### 2. Jenkins Pipeline as Code
- Declarative Pipeline 작성
- Scripted Pipeline 작성
- 멀티 브랜치 파이프라인
- Shared Libraries 활용
- 병렬 실행 및 스테이징
- 승인 프로세스 통합

### 3. ArgoCD Application 설정
- GitOps 기반 배포
- Application 매니페스트 작성
- Sync Policies 설정
- Health Check 구성
- 자동/수동 동기화 전략
- 멀티 클러스터 배포

### 4. Blue-Green 배포 전략
- 무중단 배포
- 트래픽 전환
- 롤백 메커니즘
- 환경 검증
- Smoke 테스트

### 5. Canary 배포 전략
- 점진적 트래픽 증가
- 메트릭 기반 자동 승격
- 자동 롤백
- A/B 테스트 지원
- 트래픽 분할

### 6. 보안 및 품질
- 정적 코드 분석 (SonarQube, ESLint)
- 의존성 취약점 스캔
- 컨테이너 이미지 스캔 (Trivy, Clair)
- 시크릿 관리 (Vault, Sealed Secrets)
- 코드 커버리지 측정

## Workflow

### 1. 요구사항 분석
- 프로젝트 기술 스택 파악
- 배포 환경 이해 (클라우드, 온프레미스)
- 배포 빈도 및 전략 결정
- 보안 요구사항 확인
- 테스트 전략 수립

### 2. 파이프라인 설계
- CI 단계 정의 (빌드, 테스트, 스캔)
- CD 단계 정의 (배포, 검증)
- 환경별 워크플로우 구성
- 승인 프로세스 설계
- 알림 및 모니터링 통합

### 3. 구현
- 파이프라인 코드 작성
- 시크릿 및 환경변수 설정
- 배포 전략 구현
- 테스트 자동화 통합
- 문서화

### 4. 검증 및 최적화
- 파이프라인 실행 테스트
- 성능 최적화 (캐싱, 병렬화)
- 보안 스캔 결과 검토
- 롤백 시나리오 테스트
- 모니터링 대시보드 구성

## Checklist

### GitHub Actions
- [ ] 워크플로우 트리거 정의 (push, PR, schedule)
- [ ] Job 의존성 및 순서 설정
- [ ] 환경변수 및 시크릿 구성
- [ ] 캐싱 전략 적용
- [ ] 아티팩트 저장 및 공유
- [ ] Matrix 전략 활용 (필요시)
- [ ] 재사용 가능한 워크플로우 작성

### Jenkins Pipeline
- [ ] Jenkinsfile 작성 (Declarative/Scripted)
- [ ] Stage 분리 및 병렬 실행
- [ ] 환경별 파라미터화
- [ ] 크리덴셜 관리
- [ ] Post 액션 정의 (성공/실패)
- [ ] Shared Library 활용 (재사용)
- [ ] 승인 게이트 구현

### ArgoCD
- [ ] Application 리소스 정의
- [ ] Sync Policy 설정
- [ ] Health Check 구성
- [ ] Auto Sync 활성화/비활성화
- [ ] Prune 정책 설정
- [ ] 멀티 소스 지원 (필요시)
- [ ] RBAC 설정

### 배포 전략
- [ ] Blue-Green/Canary 선택
- [ ] 트래픽 분할 비율 정의
- [ ] Health Check 엔드포인트 설정
- [ ] 메트릭 수집 및 모니터링
- [ ] 자동 롤백 조건 정의
- [ ] Smoke 테스트 작성
- [ ] 배포 시간 계획

### 보안
- [ ] 시크릿 안전하게 관리
- [ ] 코드 정적 분석
- [ ] 의존성 취약점 스캔
- [ ] 컨테이너 이미지 스캔
- [ ] 최소 권한 원칙 적용
- [ ] 감사 로그 활성화

## Usage Examples

### Example 1: GitHub Actions - Node.js CI/CD

**요청:** "Node.js 프로젝트를 위한 GitHub Actions 워크플로우를 만들어주세요"

**생성된 워크플로우:**

```yaml
name: Node.js CI/CD

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

env:
  NODE_VERSION: '18.x'
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        node-version: [16.x, 18.x, 20.x]

    steps:
    - uses: actions/checkout@v4

    - name: Setup Node.js ${{ matrix.node-version }}
      uses: actions/setup-node@v4
      with:
        node-version: ${{ matrix.node-version }}
        cache: 'npm'

    - name: Install dependencies
      run: npm ci

    - name: Run linter
      run: npm run lint

    - name: Run tests
      run: npm test

    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        token: ${{ secrets.CODECOV_TOKEN }}

  security-scan:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4

    - name: Run Snyk security scan
      uses: snyk/actions/node@master
      env:
        SNYK_TOKEN: ${{ secrets.SNYK_TOKEN }}

    - name: Run Trivy vulnerability scanner
      uses: aquasecurity/trivy-action@master
      with:
        scan-type: 'fs'
        scan-ref: '.'
        format: 'sarif'
        output: 'trivy-results.sarif'

  build:
    needs: [test, security-scan]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'

    steps:
    - uses: actions/checkout@v4

    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v3

    - name: Log in to Container Registry
      uses: docker/login-action@v3
      with:
        registry: ${{ env.REGISTRY }}
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    - name: Extract metadata
      id: meta
      uses: docker/metadata-action@v5
      with:
        images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
        tags: |
          type=ref,event=branch
          type=semver,pattern={{version}}
          type=semver,pattern={{major}}.{{minor}}
          type=sha,prefix={{branch}}-

    - name: Build and push Docker image
      uses: docker/build-push-action@v5
      with:
        context: .
        push: true
        tags: ${{ steps.meta.outputs.tags }}
        labels: ${{ steps.meta.outputs.labels }}
        cache-from: type=gha
        cache-to: type=gha,mode=max

  deploy-staging:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: staging
      url: https://staging.example.com

    steps:
    - name: Deploy to staging
      run: |
        echo "Deploying to staging environment"
        # kubectl set image deployment/myapp myapp=${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ github.sha }}

  deploy-production:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment:
      name: production
      url: https://example.com
    if: github.ref == 'refs/heads/main'

    steps:
    - name: Deploy to production
      run: |
        echo "Deploying to production environment"
        # kubectl set image deployment/myapp myapp=${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ github.sha }}
```

### Example 2: Jenkins Declarative Pipeline

**요청:** "Java Spring Boot 프로젝트를 위한 Jenkins 파이프라인을 작성해주세요"

**생성된 Jenkinsfile:**

```groovy
pipeline {
    agent any

    environment {
        MAVEN_OPTS = '-Dmaven.repo.local=.m2/repository'
        DOCKER_REGISTRY = 'your-registry.com'
        IMAGE_NAME = 'myapp'
        SONAR_PROJECT_KEY = 'my-project'
    }

    tools {
        maven 'Maven 3.9.0'
        jdk 'JDK 17'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build') {
            steps {
                sh 'mvn clean compile'
            }
        }

        stage('Test') {
            parallel {
                stage('Unit Tests') {
                    steps {
                        sh 'mvn test'
                    }
                    post {
                        always {
                            junit '**/target/surefire-reports/*.xml'
                        }
                    }
                }

                stage('Integration Tests') {
                    steps {
                        sh 'mvn verify -DskipUnitTests'
                    }
                }
            }
        }

        stage('Code Quality') {
            steps {
                withSonarQubeEnv('SonarQube') {
                    sh '''
                        mvn sonar:sonar \
                          -Dsonar.projectKey=${SONAR_PROJECT_KEY} \
                          -Dsonar.coverage.jacoco.xmlReportPaths=target/site/jacoco/jacoco.xml
                    '''
                }
            }
        }

        stage('Quality Gate') {
            steps {
                timeout(time: 5, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }

        stage('Security Scan') {
            parallel {
                stage('Dependency Check') {
                    steps {
                        dependencyCheck additionalArguments: '--format HTML --format XML',
                                      odcInstallation: 'OWASP Dependency-Check'
                    }
                }

                stage('Trivy Scan') {
                    steps {
                        sh 'trivy fs --security-checks vuln .'
                    }
                }
            }
        }

        stage('Package') {
            steps {
                sh 'mvn package -DskipTests'
            }
            post {
                success {
                    archiveArtifacts artifacts: '**/target/*.jar', fingerprint: true
                }
            }
        }

        stage('Build Docker Image') {
            when {
                branch 'main'
            }
            steps {
                script {
                    def imageTag = "${env.BUILD_NUMBER}"
                    docker.build("${DOCKER_REGISTRY}/${IMAGE_NAME}:${imageTag}")
                    docker.build("${DOCKER_REGISTRY}/${IMAGE_NAME}:latest")
                }
            }
        }

        stage('Push Docker Image') {
            when {
                branch 'main'
            }
            steps {
                script {
                    docker.withRegistry("https://${DOCKER_REGISTRY}", 'docker-credentials') {
                        def imageTag = "${env.BUILD_NUMBER}"
                        docker.image("${DOCKER_REGISTRY}/${IMAGE_NAME}:${imageTag}").push()
                        docker.image("${DOCKER_REGISTRY}/${IMAGE_NAME}:latest").push()
                    }
                }
            }
        }

        stage('Deploy to Staging') {
            when {
                branch 'main'
            }
            steps {
                script {
                    kubernetesDeploy(
                        configs: 'k8s/staging/*.yaml',
                        kubeconfigId: 'kubeconfig-staging'
                    )
                }
            }
        }

        stage('Approval for Production') {
            when {
                branch 'main'
            }
            steps {
                input message: 'Deploy to production?', ok: 'Deploy'
            }
        }

        stage('Deploy to Production') {
            when {
                branch 'main'
            }
            steps {
                script {
                    kubernetesDeploy(
                        configs: 'k8s/production/*.yaml',
                        kubeconfigId: 'kubeconfig-production'
                    )
                }
            }
        }
    }

    post {
        always {
            cleanWs()
        }
        success {
            slackSend color: 'good', message: "Build Successful: ${env.JOB_NAME} ${env.BUILD_NUMBER}"
        }
        failure {
            slackSend color: 'danger', message: "Build Failed: ${env.JOB_NAME} ${env.BUILD_NUMBER}"
        }
    }
}
```

### Example 3: ArgoCD with Canary Deployment

**요청:** "Canary 배포를 위한 ArgoCD Application을 설정해주세요"

**생성된 매니페스트:**

```yaml
# argocd-application.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: myapp
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/myorg/myapp-manifests
    targetRevision: HEAD
    path: overlays/production
  destination:
    server: https://kubernetes.default.svc
    namespace: production
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
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

```yaml
# rollout.yaml - Argo Rollouts Canary
apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: myapp
spec:
  replicas: 5
  strategy:
    canary:
      steps:
      - setWeight: 20
      - pause: {duration: 5m}
      - setWeight: 40
      - pause: {duration: 5m}
      - setWeight: 60
      - pause: {duration: 5m}
      - setWeight: 80
      - pause: {duration: 5m}
      canaryService: myapp-canary
      stableService: myapp-stable
      trafficRouting:
        istio:
          virtualService:
            name: myapp
            routes:
            - primary
      analysis:
        templates:
        - templateName: success-rate
        startingStep: 2
        args:
        - name: service-name
          value: myapp-canary
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
        ports:
        - name: http
          containerPort: 8080
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

```yaml
# analysis-template.yaml
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
    provider:
      prometheus:
        address: http://prometheus:9090
        query: |
          sum(rate(
            http_requests_total{service="{{args.service-name}}",status!~"5.."}[5m]
          )) /
          sum(rate(
            http_requests_total{service="{{args.service-name}}"}[5m]
          ))
```

## Integration Points

### 다른 스킬과의 연계

- **developer**: CI/CD에서 실행할 코드 생성
- **tester**: 파이프라인에 통합할 테스트 작성
- **architecture-advisor**: 배포 아키텍처 자문
- **api-designer**: API 배포 전략 수립
- **code-reviewer**: 파이프라인 코드 리뷰

### 외부 도구 통합

- **SonarQube**: 코드 품질 분석
- **Snyk/Trivy**: 보안 취약점 스캔
- **Prometheus/Grafana**: 메트릭 및 모니터링
- **Slack/Teams**: 알림
- **Vault**: 시크릿 관리
- **Artifactory/Nexus**: 아티팩트 저장소

## References

- `references/github-actions-patterns.md` - GitHub Actions 패턴 및 예제
- `references/jenkins-patterns.md` - Jenkins 파이프라인 패턴
- `references/argocd-patterns.md` - ArgoCD GitOps 패턴
- `references/deployment-strategies.md` - 배포 전략 상세 가이드

## Best Practices

1. **Infrastructure as Code**: 모든 파이프라인을 코드로 관리
2. **재사용성**: 공통 작업은 재사용 가능한 컴포넌트로 분리
3. **보안 우선**: 시크릿 안전 관리, 스캔 통합
4. **빠른 피드백**: 빌드 시간 최적화, 병렬 실행
5. **자동화**: 수동 개입 최소화
6. **모니터링**: 파이프라인 메트릭 수집 및 분석
7. **문서화**: 파이프라인 동작 및 설정 문서화
8. **테스트**: 파이프라인 자체도 테스트
