import boto3
from decouple import config

access_key = config("access_key_id")
secret_key = config("secret_access_key")

emr_serverless = boto3.client('emr-serverless', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                              region_name="ap-northeast-2")


def create_emr_cluster():
    cluster_generated_response = emr_serverless.create_application(
        type='SPARK',
        name='serverless-spark',
        releaseLabel="emr-6.6.0",
        initialCapacity=
        {
            "DRIVER": {
                "workerCount": 2,
                "workerConfiguration": {
                    "cpu": "2vCPU",
                    "memory": "4GB"
                }
            },
            "EXECUTOR": {
                "workerCount": 20,
                "workerConfiguration": {
                    "cpu": "4vCPU",
                    "memory": "8GB",
                    "disk": "50GB"
                }
            }
        },
        maximumCapacity={
            "cpu": "100vCPU",
            "memory": "500GB",
            "disk": "1500GB"
        },
        networkConfiguration={
            "subnetIds": ["subnet-079171761f3387134", "subnet-091dd7fc0b400d476"],
            "securityGroupIds": ["sg-00a4705e6a09bfdfa"]
        }
    )

    return cluster_generated_response["applicationId"]


def start_cluster(app_id: str):
    emr_serverless.start_application(
        applicationId=app_id
    )


def run_spark_job(app_id: str):
    emr_serverless.start_job_run(
        applicationId=app_id,
        executionRoleArn='arn:aws:iam::502137442150:role/emr-serverless-job-role',
        jobDriver={
            "sparkSubmit": {
                "entryPoint": "s3://clickstreamanalysis/emr-container/ip-groupby/spark_run.py",
                "sparkSubmitParameters": "--jars s3://clickstreamanalysis/extra-jars/mysql-connector-java-8.0.29-tidb-1.0.2.jar --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.cores=2 --conf spark.executor.memory=8g --conf spark.executor.instances=20"
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://clickstreamanalysis/logs/"
                }
            }
        }
    )


def stop_cluster(app_id: str):
    emr_serverless.stop_application(
        applicationId=app_id
    )


def delete_emr_cluster(app_id: str):
    emr_serverless.delete_application(applicationId=app_id)


if __name__ == "__main__":
    # application_id = create_emr_cluster()
    #
    # print(application_id)

    application_id = '00f83stvm1nl1r2p'

    # start_cluster(app_id=application_id)
    #
    # run_spark_job(app_id=application_id)
    #

    # stop_cluster(app_id=application_id)

    delete_emr_cluster(app_id=application_id)
