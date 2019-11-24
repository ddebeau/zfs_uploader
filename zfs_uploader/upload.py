import boto3


def get_s3_resource(region, access_key, secret_key):
    """ Get s3 resouce. """
    s3 = boto3.resource(service_name='s3',
                        region_name=region,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
    return s3
