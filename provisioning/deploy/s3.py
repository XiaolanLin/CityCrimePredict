import argparse
import os

import boto3

from utils import get_env_value


def upload_jar(jar_path):
    """
    Upload runnable jar file to S3
    The jar will be uploaded to directory 'BUCKET/project/jar'
    """

    region = get_env_value('REGION', 'us-east-1')
    bucket_name = get_env_value('BUCKET')
    access_key = get_env_value('AWS_ACCESS_KEY')
    secret_key = get_env_value('AWS_SECRET_KEY')

    s3 = boto3.resource('s3',
                        region_name=region,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
    # Upload a new file
    jar = open(jar_path, 'rb')
    key = os.path.join('project/jar', os.path.basename(jar.name))
    print('Uploading %s to s3://%s/%s' % (jar.name, bucket_name, key))
    s3.Bucket(bucket_name).put_object(Key=key, Body=jar)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("jar_path", help="Path to the runnable jar file")
    args = parser.parse_args()
    upload_jar(args.jar_path)
