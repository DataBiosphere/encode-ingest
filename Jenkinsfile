pipeline {
    // Docker needed to run tests.
    agent { label 'docker' }

    options {
        timestamps()
        ansiColor('xterm')
        disableConcurrentBuilds()
    }
    environment {
        PATH = "${tool('vault')}:${tool('sbt')}:$PATH"
        // Some wiring is broken between the custom-tools plugin and
        // the pipeline plugin which prevents these vars from being
        // injected when pulling in the custom 'vault' tool.
        VAULT_ADDR = 'https://clotho.broadinstitute.org:8200'
        VAULT_TOKEN_PATH = '/etc/vault-token-monster'
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
        stage('Test') {
            steps {
                sh 'sbt "set ThisBuild/coverageEnabled := true" test'
            }
        }
        stage('Integration test') {
            steps {
                sh 'sbt "set ThisBuild/coverageEnabled := true" IntegrationTest/test'
            }
        }
    }
    post {
        always {
            junit allowEmptyResults: true, testResults: '**/target/test-reports/*'
        }
        success {
            script {
                def vaultPath = 'secret/dsde/monster/dev/codecov/encode-ingest'
                def codecov = [
                        '1>&2 bash <(curl -s https://codecov.io/bash)',
                        '-K',
                        '-t ${CODECOV_TOKEN}',
                        "-s ${env.WORKSPACE}/target",
                        '-C $(git rev-parse HEAD)',
                        "-b ${env.BUILD_NUMBER}"
                ].join(' ')
                def parts = [
                        '#!/bin/bash',
                        'set +x',
                        'echo Publishing code coverage...',
                        'export VAULT_TOKEN=$(cat $VAULT_TOKEN_PATH)',
                        "CODECOV_TOKEN=\$(vault read -field=token $vaultPath)",
                        'sbt coverageAggregate',
                        "if [ -z '${env.CHANGE_ID}' ]; then ${codecov} -B ${env.BRANCH_NAME}; else ${codecov} -P ${env.CHANGE_ID}; fi"
                ]
                sh parts.join('\n')
            }
        }
        cleanup {
            cleanWs()
        }
    }
}
