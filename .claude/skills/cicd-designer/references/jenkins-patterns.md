# Jenkins Pipeline 패턴

Jenkins를 활용한 효과적인 CI/CD 파이프라인 구축 패턴입니다.

## Declarative vs Scripted Pipeline

### Declarative Pipeline (권장)

```groovy
pipeline {
    agent any

    environment {
        APP_NAME = 'myapp'
        VERSION = "${env.BUILD_NUMBER}"
    }

    stages {
        stage('Build') {
            steps {
                sh 'mvn clean compile'
            }
        }

        stage('Test') {
            steps {
                sh 'mvn test'
            }
        }
    }

    post {
        always {
            cleanWs()
        }
    }
}
```

### Scripted Pipeline

```groovy
node {
    try {
        stage('Build') {
            sh 'mvn clean compile'
        }

        stage('Test') {
            sh 'mvn test'
        }
    } catch (e) {
        currentBuild.result = 'FAILURE'
        throw e
    } finally {
        cleanWs()
    }
}
```

## Agent 패턴

### 다양한 Agent 활용

```groovy
pipeline {
    // 모든 스테이지에 기본 agent
    agent none

    stages {
        stage('Build on Docker') {
            agent {
                docker {
                    image 'maven:3.9.0-eclipse-temurin-17'
                    args '-v $HOME/.m2:/root/.m2'
                }
            }
            steps {
                sh 'mvn clean package'
            }
        }

        stage('Test on Specific Node') {
            agent {
                label 'linux && x86'
            }
            steps {
                sh 'npm test'
            }
        }

        stage('Deploy on Kubernetes') {
            agent {
                kubernetes {
                    yaml '''
                        apiVersion: v1
                        kind: Pod
                        spec:
                          containers:
                          - name: kubectl
                            image: bitnami/kubectl:latest
                    '''
                }
            }
            steps {
                container('kubectl') {
                    sh 'kubectl apply -f deployment.yaml'
                }
            }
        }
    }
}
```

## 병렬 실행

```groovy
pipeline {
    agent any

    stages {
        stage('Parallel Tests') {
            parallel {
                stage('Unit Tests') {
                    steps {
                        sh 'npm run test:unit'
                    }
                }

                stage('Integration Tests') {
                    steps {
                        sh 'npm run test:integration'
                    }
                }

                stage('E2E Tests') {
                    steps {
                        sh 'npm run test:e2e'
                    }
                }
            }
        }
    }
}
```

## 파라미터화

```groovy
pipeline {
    agent any

    parameters {
        choice(
            name: 'ENVIRONMENT',
            choices: ['dev', 'staging', 'production'],
            description: 'Deployment environment'
        )

        string(
            name: 'VERSION',
            defaultValue: 'latest',
            description: 'Version to deploy'
        )

        booleanParam(
            name: 'RUN_TESTS',
            defaultValue: true,
            description: 'Run tests before deployment'
        )

        text(
            name: 'RELEASE_NOTES',
            defaultValue: '',
            description: 'Release notes'
        )
    }

    stages {
        stage('Test') {
            when {
                expression { params.RUN_TESTS == true }
            }
            steps {
                sh 'npm test'
            }
        }

        stage('Deploy') {
            steps {
                script {
                    echo "Deploying ${params.VERSION} to ${params.ENVIRONMENT}"
                    sh "./deploy.sh ${params.ENVIRONMENT} ${params.VERSION}"
                }
            }
        }
    }
}
```

## When 조건

```groovy
pipeline {
    agent any

    stages {
        stage('Deploy to Production') {
            when {
                allOf {
                    branch 'main'
                    environment name: 'DEPLOY_ENV', value: 'production'
                    expression { currentBuild.result == null }
                }
            }
            steps {
                sh './deploy-production.sh'
            }
        }

        stage('Deploy to Staging') {
            when {
                anyOf {
                    branch 'develop'
                    branch 'staging'
                }
            }
            steps {
                sh './deploy-staging.sh'
            }
        }

        stage('Tagged Release') {
            when {
                tag pattern: "v\\d+\\.\\d+\\.\\d+", comparator: "REGEXP"
            }
            steps {
                sh './create-release.sh'
            }
        }
    }
}
```

## Shared Library 활용

### 라이브러리 정의

```groovy
// vars/buildDockerImage.groovy
def call(Map config) {
    def imageName = config.imageName
    def tag = config.tag ?: 'latest'
    def dockerfile = config.dockerfile ?: 'Dockerfile'

    sh """
        docker build -t ${imageName}:${tag} -f ${dockerfile} .
        docker push ${imageName}:${tag}
    """
}

// vars/deployToK8s.groovy
def call(String environment, String manifest) {
    withKubeConfig([credentialsId: "kubeconfig-${environment}"]) {
        sh "kubectl apply -f ${manifest}"
    }
}
```

### 라이브러리 사용

```groovy
@Library('my-shared-library') _

pipeline {
    agent any

    stages {
        stage('Build Image') {
            steps {
                buildDockerImage(
                    imageName: 'myapp',
                    tag: "${env.BUILD_NUMBER}"
                )
            }
        }

        stage('Deploy') {
            steps {
                deployToK8s('production', 'k8s/deployment.yaml')
            }
        }
    }
}
```

## 크리덴셜 관리

```groovy
pipeline {
    agent any

    stages {
        stage('Deploy') {
            steps {
                withCredentials([
                    usernamePassword(
                        credentialsId: 'docker-hub',
                        usernameVariable: 'DOCKER_USER',
                        passwordVariable: 'DOCKER_PASS'
                    ),
                    string(
                        credentialsId: 'api-key',
                        variable: 'API_KEY'
                    ),
                    file(
                        credentialsId: 'kubeconfig',
                        variable: 'KUBECONFIG_FILE'
                    )
                ]) {
                    sh '''
                        echo $DOCKER_PASS | docker login -u $DOCKER_USER --password-stdin
                        export KUBECONFIG=$KUBECONFIG_FILE
                        kubectl apply -f deployment.yaml
                    '''
                }
            }
        }
    }
}
```

## 멀티 브랜치 파이프라인

```groovy
// Jenkinsfile
pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'mvn clean package'
            }
        }

        stage('Test') {
            steps {
                sh 'mvn test'
            }
        }

        stage('Deploy') {
            steps {
                script {
                    if (env.BRANCH_NAME == 'main') {
                        sh './deploy-production.sh'
                    } else if (env.BRANCH_NAME == 'develop') {
                        sh './deploy-staging.sh'
                    } else if (env.BRANCH_NAME.startsWith('feature/')) {
                        sh './deploy-dev.sh'
                    }
                }
            }
        }
    }
}
```

## Post Actions

```groovy
pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'mvn clean package'
            }
        }
    }

    post {
        always {
            junit '**/target/surefire-reports/*.xml'
            archiveArtifacts artifacts: '**/target/*.jar', fingerprint: true
        }

        success {
            slackSend(
                color: 'good',
                message: "Build Successful: ${env.JOB_NAME} ${env.BUILD_NUMBER}"
            )
        }

        failure {
            slackSend(
                color: 'danger',
                message: "Build Failed: ${env.JOB_NAME} ${env.BUILD_NUMBER}"
            )
            emailext(
                subject: "Build Failed: ${env.JOB_NAME}",
                body: "Build ${env.BUILD_NUMBER} failed. Check console output.",
                to: 'team@example.com'
            )
        }

        unstable {
            echo 'Build is unstable'
        }

        cleanup {
            cleanWs()
        }
    }
}
```

## Input 승인

```groovy
pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'mvn clean package'
            }
        }

        stage('Deploy to Staging') {
            steps {
                sh './deploy-staging.sh'
            }
        }

        stage('Approval') {
            steps {
                input message: 'Deploy to production?',
                      ok: 'Deploy',
                      submitter: 'admin,devops',
                      parameters: [
                          choice(
                              name: 'CONFIRM',
                              choices: ['Yes', 'No'],
                              description: 'Confirm deployment'
                          )
                      ]
            }
        }

        stage('Deploy to Production') {
            steps {
                sh './deploy-production.sh'
            }
        }
    }
}
```

## 타임아웃 및 재시도

```groovy
pipeline {
    agent any

    options {
        timeout(time: 30, unit: 'MINUTES')
        retry(3)
        timestamps()
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }

    stages {
        stage('Build with Timeout') {
            options {
                timeout(time: 10, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean package'
            }
        }

        stage('Retry on Failure') {
            options {
                retry(3)
            }
            steps {
                sh './flaky-test.sh'
            }
        }
    }
}
```

## 체크리스트

- [ ] Declarative Pipeline 사용
- [ ] 적절한 Agent 선택
- [ ] 병렬 실행 활용
- [ ] Shared Library로 재사용성 확보
- [ ] 크리덴셜 안전하게 관리
- [ ] Post Actions 정의
- [ ] 승인 프로세스 구현 (필요시)
- [ ] 타임아웃 설정
- [ ] 로그 보관 정책 설정
- [ ] 알림 통합
