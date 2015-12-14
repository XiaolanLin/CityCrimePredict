Plotting Scripts
===

## Required Modules

We use [pandas](http://pandas.pydata.org) and [matplotlib](http://matplotlib.org) to plot graphes.

```Shell
# Install requirements
pip install -r requirements.txt
```

## Usage

### Download Hadoop outputs from S3
You can use [s3cmd](http://s3tools.org/s3cmd) to download output files.

```Shell
s3cmd sync --access_key=$AWS_ACCESS_KEY --secret_key=$AWS_SECRET_KEY \
--exclude '_SUCCESS' s3://$BUCKET/project/output/ output
```

### Plot count of *Category* and *PdDistrict*

```Shell
cat output/output_category/part-r-0000* | sort | python graph/plot_attrs_count.py
cat output/output_pd/part-r-0000* | sort | python graph/plot_attrs_count.py
```
