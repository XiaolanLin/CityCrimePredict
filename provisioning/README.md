Provisioning Scripts
===

## Virtual Environment for Running Scripts

We recommand you to use virtual environment, which can help you get rid of `sudo` and separate from your system Python environment. You may need to install following modules:

* [virtualenv](https://virtualenv.pypa.io/en/latest/)
* [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/)

Please read documentations for installations and configurations.

### Install Required Modules
You can install required Python modules by the following commands:

```Shell
# Switch to virtualenv 'aws', require virtualenvwrapper
workon aws
# Install requirements
pip install -r requirements.txt
```

## Usage

### Configurations

You have to config before running scripts. To protect your privacy, following variables need to be set manually.(I don't want to put them in code and upload them to Github.)

```Shell
export BUCKET='mybucket'
export REGION='us-west-2'
export AWS_ACCESS_KEY='xxxxxxxxx'
export AWS_SECRET_KEY='xxxxxxxxx'
export EC2_KEYNAME='key'
```

### Upload Runnable JAR to S3
Upload your runnable jar to the directory `BUCKET/project/jars`.

```Shell
python provisioning/deploy/s3.py JAR_PATH
```

### Launch AWS EMR
You may have to modify EMR cluster configuration and implement `main` function in `emr.py`.

```Shell
python provisioning/deploy/emr.py JAR_NAME
```

### Download Hadoop Output
You can use [s3cmd](http://s3tools.org/s3cmd) to download output files.

```Shell
s3cmd sync --access_key=$AWS_ACCESS_KEY --secret_key=$AWS_SECRET_KEY \
--exclude '_SUCCESS' s3://$BUCKET/project/output/ output
```
