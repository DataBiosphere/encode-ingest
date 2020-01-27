pipeline {
    // Docker needed to run tests.
    agent { label 'docker' }

    options {
        timestamps()
        ansiColor('xterm')
        disableConcurrentBuilds()
    }
    environment {
        PATH = "${tool('sbt')}:$PATH"
    }
    stages {
        stage('Check formatting') {
            steps {
                sh 'sbt scalafmtCheckAll'
            }
        }
        stage('Compile') {
            steps {
                sh 'sbt Compile/compile Test/compile IntegrationTest/compile'
            }
        }
        //stage('Test') {
        //    steps {
        //        sh 'sbt test'
        //    }
        //}
        //stage('Integration test') {
        //    steps {
        //        sh 'sbt IntegrationTest/test'
        //    }
        //}
    }
    post {
        //always {
        //    junit '**/target/test-reports/*'
        //}
        cleanup {
            cleanWs()
        }
    }
}
