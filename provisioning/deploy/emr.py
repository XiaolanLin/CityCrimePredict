#!/usr/bin/env python

import sys

import boto3

from utils import get_env_value
"""
http://boto3.readthedocs.org/en/latest/reference/services/emr.html
"""


def create_job(job_name,
               steps=[],
               zone='us-east-1b',
               bucket_name=None,
               master_type='m1.medium',
               slave_type='m1.medium',
               num_instances=3):

    bucket_name = bucket_name or get_env_value('BUCKET')
    access_key = get_env_value('AWS_ACCESS_KEY')
    secret_key = get_env_value('AWS_SECRET_KEY')
    ec2_key = get_env_value('EC2_KEYNAME')

    log_uri = 's3://%s/project/logs' % bucket_name

    emr = boto3.client('emr',
                       region_name=zone[:-1],
                       aws_access_key_id=access_key,
                       aws_secret_access_key=secret_key)

    emr.run_job_flow(Name=job_name,
                     LogUri=log_uri,
                     ReleaseLabel='emr-4.1.0',
                     Instances={
                         'InstanceGroups': [
                             {
                                 'Name': 'Master',
                                 'Market': 'ON_DEMAND',
                                 'InstanceRole': 'MASTER',
                                 'InstanceType': master_type,
                                 'InstanceCount': 1,
                             }, {
                                 'Name': 'Worker',
                                 'Market': 'ON_DEMAND',
                                 'InstanceRole': 'CORE',
                                 'InstanceType': slave_type,
                                 'InstanceCount': num_instances - 1,
                             }
                         ],
                         'Ec2KeyName': ec2_key,
                         'Placement': {
                             'AvailabilityZone': zone
                         },
                         'KeepJobFlowAliveWhenNoSteps': False,
                         'TerminationProtected': True,
                         'HadoopVersion': '2.6.0',
                     },
                     Steps=steps,
                     VisibleToAllUsers=True,
                     JobFlowRole='EMR_EC2_DefaultRole',
                     ServiceRole='EMR_DefaultRole')


def create_custom_jar_step(step_name, jar_name, args=[], bucket_name=None):
    """
    Create custom jar step for EMR cluster
    """
    bucket_name = bucket_name or get_env_value('BUCKET')
    jar = 's3://%s/project/jars/%s' % (bucket_name, jar_name)
    step = {
        'Name': step_name,
        'ActionOnFailure': 'TERMINATE_JOB_FLOW',
        'HadoopJarStep': {
            'Jar': jar,
            # 'MainClass': 'string',
            'Args': args
        }
    }
    print('Created custom jar step: %s' % jar_name)
    return step


def run(jar_name):
    bucket_name = get_env_value('BUCKET')
    input_path = 's3://%s/project/data' % bucket_name
    output_path = 's3://%s/project/output' % bucket_name
    # Implement main function by adding custom jar steps
    step1 = create_custom_jar_step('StatisticsAnalysis',
                                   jar_name,
                                   args=[input_path, output_path],
                                   bucket_name=bucket_name)
    create_job('Hadoop-Crime-Analysis', steps=[step1, ])
    print('\nJob Hadoop-Crime-Analysis is running.\n' +
          'Go to AWS Console to check its status.')


if __name__ == '__main__':
    run(sys.argv[1])
