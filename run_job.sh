#!/bin/bash
# write this to a json config file
terraform output -json > config.json

APPLICATION_ID=$(cat config.json | jq -r .application_id.value)
EXECUTION_ARN=$(cat config.json | jq -r .execution_role_arn.value)

ENTRYPOINT=s3://dp-emr-serverless/scripts/spark_serverless-assembly-0.1.0-SNAPSHOT.jar
CLASS=com.teletracking.dataplatform.samples.SampleJob
OUTPUT_DIR="s3://dp-emr-serverless/output/tables/foobar"


aws emr-serverless start-job-run \
    --application-id ${APPLICATION_ID} \
    --execution-role-arn ${EXECUTION_ARN} \
    --job-driver "{
        \"sparkSubmit\": {
            \"entryPoint\": \"${ENTRYPOINT}\",
            \"entryPointArguments\": [\"${OUTPUT_DIR}\"],
            \"sparkSubmitParameters\": \"--class ${CLASS} --conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1\"
        }
    }"
