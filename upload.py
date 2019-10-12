import boto3


def get_s3_client(config):
    """ Get s3 client. """
    s3 = boto3.client(service_name='s3',
                      region_name=config.region,
                      aws_access_key_id=config.access_key,
                      aws_secret_access_key=config.secret_key)
    return s3
