pipeline {
   options {
        disableConcurrentBuilds()
   }
   agent { label 'dss-plugin-tests2'}
   environment {
        PLUGIN_INTEGRATION_TEST_INSTANCE="$HOME/instance_config.json"
        DKUINSTALLDIR = "${WORKSPACE}/dataiku-dss-14.1.0"
        JAVA_HOME = "/usr/lib/jvm/java-17-openjdk"
        PATH = "${JAVA_HOME}/bin:${PATH}"
   }
   stages {
      stage('Download DSS') {
         steps {
            sh 'echo "Downloading DSS"'
            sh '''
               DSS_URL="https://cdn.downloads.dataiku.com/public/studio/14.1.0/dataiku-dss-14.1.0.tar.gz"
               
               echo "Downloading DSS (version 14.1.0)"
               wget -q ${DSS_URL} -O dataiku-dss.tar.gz
               
               echo "Extracting DSS"
               tar -xzf dataiku-dss.tar.gz
               
               echo "DSS installed in: ${DKUINSTALLDIR}"
            '''
            sh 'echo "Done downloading DSS"'
         }
      }
      stage('Run Unit Tests') {
         steps {
            sh 'echo "Running unit tests"'
            catchError(stageResult: 'FAILURE') {
            sh """
               make unit-tests
               """
            }
            sh 'echo "Done with unit tests"'
         }
      }
      //stage('Run Integration Tests') {
      //   when { environment name: 'INTEGRATION_TEST_FILES_STATUS_CODE', value: "0"}
      //   steps {
      //      sh 'echo "Running integration tests"'
      //      catchError(stageResult: 'FAILURE') {
      //      sh """
      //         make integration-tests
      //         """
      //      }
      //      sh 'echo "Done with integration tests"'
      //   }
      //}
   }
   post {
     always {
        script {
           allure([
                    includeProperties: false,
                    jdk: '',
                    properties: [],
                    reportBuildPolicy: 'ALWAYS',
                    results: [[path: 'tests/allure_report']]
            ])

            def status = currentBuild.currentResult
            sh "file_name=\$(echo ${env.JOB_NAME} | tr '/' '-').status; touch \$file_name; echo \"${env.BUILD_URL};${env.CHANGE_TITLE};${env.CHANGE_AUTHOR};${env.CHANGE_URL};${env.BRANCH_NAME};${status};\" >> $HOME/daily-statuses/\$file_name"
            cleanWs()
        }
     }
   }
}
