pipeline {
    agent any

    tools {
        maven "MVN3"
    }
    environment {
        DOCKERHUB_CREDENTIALS = credentials('dockerhub')
    }

    stages {
        stage('Build') {
            steps {
                git 'git@github.com:oberasoftware/jasdb.git'

                sh "mvn clean install -Dmaven.test.skip=true"
            }

            post {
                success {
                    archiveArtifacts '**/target/*.jar'
                }
            }
        }
        stage('login to dockerhub') {
            steps {
                sh 'echo $DOCKERHUB_CREDENTIALS_PSW | docker login -u $DOCKERHUB_CREDENTIALS_USR --password-stdin'
            }
        }
        stage("Docker") {
            steps {
                sh "docker build . -t renarj/jasdb:latest"
                sh "docker push renarj/jasdb:latest"
            }
        }
    }
}
