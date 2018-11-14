// comment
pipeline {
    agent any

    options {
        disableConcurrentBuilds()
        timestamps()
    }

    environment {
        GIT_COMMIT = sh(returnStdout: true, script: "git rev-parse HEAD").trim()
    }

    stages {
        stage('Build Docker image') {
            steps {
                sh "docker build -t sunbit/schema-registry:$GIT_COMMIT ."
            }
        }

        stage('Push Docker image') {
            steps {
                sh "docker push sunbit/schema-registry:$GIT_COMMIT"
            }
        }
    }
}
