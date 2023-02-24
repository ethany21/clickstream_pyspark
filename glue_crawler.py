import boto3
from decouple import config

access_key = config("access_key_id")
secret_key = config("secret_access_key")

glue_crawler = boto3.client('glue', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                            region_name="ap-northeast-2")


def create_crawler():
    response = glue_crawler.create_crawler(
        Name='clickstream_glue_crawler',
        Role='arn:aws:iam::502137442150:role/service-role/AWSGlueServiceRole-clickstream-crawler',
        DatabaseName='clickstream',
        Targets={
            'CatalogTargets': [
                {
                    'DatabaseName': 'clickstream',
                    'Tables': [
                        'clickstream',
                    ]
                },
            ]
        },
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'LOG'
        },
        RecrawlPolicy={
            'RecrawlBehavior': 'CRAWL_EVERYTHING'
        }
    )

    print(response)


if __name__ == "__main__":
    # create_crawler()

    # glue_crawler.start_crawler(
    #     Name='clickstream_glue_crawler'
    # )

    # glue_crawler.stop_crawler(
    #     Name='clickstream_glue_crawler'
    # )

    glue_crawler.delete_crawler(
        Name='clickstream_glue_crawler'
    )
