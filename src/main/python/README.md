Set environment variables before running

```
export SPARK_HOME=/Users/yummin/Program/Java/lib/spark-1.5.1-bin-hadoop2.6
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH
```

### On AWS EMR

```
export SPARK_HOME=/usr/lib/spark/
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:.:$PYTHONPATH
```
