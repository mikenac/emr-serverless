from distutils.log import Log
from typing import Iterable, Mapping, Optional, Tuple
from time import sleep
import json
import boto3


class EmrServerlessJobRunner():
    """ Run EMR serverless jobs """

    JOB_RUN_INTERMEDIATE_STATES = (
        "SUBMITTED",
        "PENDING",
        "SCHEDULED",
        "RUNNING",
    )
    JOB_RUN_FAILURE_STATES = (
        "FAILED",
        "CANCELLED",
        "CANCELLING",
    )
    JOB_RUN_SUCCESS_STATES = ("SUCCESS",)
    JOB_RUN_TERMINAL_STATES = JOB_RUN_SUCCESS_STATES + JOB_RUN_FAILURE_STATES

    current_state = "UNKNOWN"

    def __init__(self, application_id: str, execution_arn: str):
        self.client = boto3.client('emr-serverless')
        self.appication_id = application_id
        self.execution_arn = execution_arn

    @staticmethod
    def __spark_conf_from_dict(configEntries: Mapping[str, str]) -> str:
        """ Convert a dictionary of key values pairs of config to 
            the string required by boto for submission """
        
        output_str = ""
        for k, v in configEntries.items():
            entry = f" --conf {k}={v}"
            output_str += entry
        return output_str
    

    def run_job(self, jobName: str, entryPoint: str,
                className: str,
                configurationEntries: Mapping[str, str],
                logPath: str,
                timeoutMinutes: int = 90,
                jobArguments: Iterable[str]=[]) ->Tuple[str, str]:
        """ Starts a job and returns the job identifier for status monitoring - non blocking.

            Parameters:
                jobName (str): the name you want to give the job
                entryPoint (str): the job jar file or python script
                className (str): the full class name for the job entry point
                configurationEntries (map[str,str]): K,V pair of spark configuation items to pass to the job. Same as the -conf in Spark Submit.
                logPath (str): the S3 URI for job logging
                timeoutMinutes (int): the max runtime for the job before EMR kills it.
                jobArguments (str[]): a list of arguments to pass to the job

            Returns:
                Tuple(application_id, job_run_id)
        """

        sparkParamString = EmrServerlessJobRunner.__spark_conf_from_dict(configEntries=configurationEntries)
     
        result = self.client.start_job_run(
            applicationId=self.appication_id,
            executionRoleArn=self.execution_arn,
            executionTimeoutMinutes=timeoutMinutes,
            jobDriver={
                'sparkSubmit': {
                    'entryPoint': entryPoint,
                    'entryPointArguments': jobArguments,
                    'sparkSubmitParameters': f"--class {className}{sparkParamString}"
                }
            },
            configurationOverrides={
                'monitoringConfiguration': {
                    's3MonitoringConfiguration': {
                        'logUri': logPath
                    }
                }
            },
            name=jobName
        )

        return (result["applicationId"], result["jobRunId"])

    def get_job_run_state(self, application_id: str, job_run_id: str) -> Tuple[str, str]:
        """ Get the job status """

        result = self.client.get_job_run(applicationId=application_id, jobRunId=job_run_id)
        
        return (result["jobRun"]["state"], result["jobRun"]["stateDetails"])


    def wait_for_job_completion(self, application_id: str,
             job_run_id: str, poll_seconds: int =5, timeout_seconds: Optional[int] = None) -> Tuple[str, str]:
        """ Wait for EMR job to exit """
        
        try_number = 0
        state = 'UNKNOWN'

        while True:
            (state, details) = self.get_job_run_state(application_id=application_id, job_run_id=job_run_id)
            if (state != self.current_state):
                print(f"State is now: {state}")
                self.current_state = state
            if (state in self.JOB_RUN_TERMINAL_STATES):
                break
            try_number += 1
            if (timeout_seconds and try_number * poll_seconds >= timeout_seconds):
                raise Exception("Timeout reached waiting for job completion.")
            sleep(poll_seconds)
        return (state, details)


if __name__=="__main__":

    config_parms = {}
    with open('./config.json') as json_file:
        config_parms = json.load(json_file)
    APPLICATION_ID = config_parms["application_id"]["value"]
    EXECUTION_ARN = config_parms["execution_role_arn"]["value"]

    JOB_CONFIG = {
        "spark.executor.cores": 2,
        "spark.executor.memory": "4g",
        "spark.driver.cores": 1,
        "spark.driver.memory": "4g",
        "spark.executor.instances": 1
    }

    JOB_ENTRY_POINT = "s3://dp-emr-serverless/scripts/spark_serverless-assembly-0.1.0-SNAPSHOT.jar"
    JOB_CLASS = "com.teletracking.dataplatform.samples.SampleJob"
    OUTPUT_DIR = "s3://dp-emr-serverless/output/tables/foobar"
    LOG_BUCKET = "s3://dp-emr-serverless/output/logs"
    TIMEOUT_MINUTES = 60

    JOB_ARGUMENTS = [ OUTPUT_DIR ]

    job_runner = EmrServerlessJobRunner(APPLICATION_ID, EXECUTION_ARN)
    (_, job_run_id) = job_runner.run_job(jobName="test job",
                                entryPoint=JOB_ENTRY_POINT, className=JOB_CLASS,
                                logPath=LOG_BUCKET,
                                timeoutMinutes=TIMEOUT_MINUTES,
                                configurationEntries=JOB_CONFIG, jobArguments=JOB_ARGUMENTS)
    print (f"Job submitted with job_id: {job_run_id}")
    (job_state, details) = job_runner.wait_for_job_completion(application_id=APPLICATION_ID, job_run_id=job_run_id)
    print(f"Job {job_run_id} finished with state: {job_state}, details: {details}")

