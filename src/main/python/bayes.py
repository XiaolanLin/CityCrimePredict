import csv
import os
import sys

from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel

from config import *


def parseLine(line):
    parts = list(csv.reader([line], delimiter=',', quotechar='"')).pop()
    # Target label
    label = Category[parts[INDEX_CATEGORY]]
    # Features
    day = DayOfWeek[parts[INDEX_DAYOFWEEK]]
    month = int(parts[INDEX_DATE].split('/')[0])
    hour = int(parts[INDEX_TIME].split(':')[0])
    day_of_month = int(parts[INDEX_DATE].split('/')[1])
    pd = PdDistrict[parts[INDEX_PD_DISTRICT]]
    # Spark only supports non-negetive value
    longitude = round(abs(float(parts[INDEX_LONGITUDE])), 3)
    latitude = round(float(parts[INDEX_LATITUDE]), 3)

    features = Vectors.dense([day, month, hour, day_of_month, pd, longitude,
                              latitude])

    return LabeledPoint(label, features)


if __name__ == '__main__':

    sc = SparkContext(appName='NaiveBayesPridiction')

    if len(sys.argv) == 3:
        train = sc.textFile(sys.argv[1]).map(parseLine)
        test = sc.textFile(sys.argv[2]).map(parseLine)
    elif len(sys.argv) == 2:
        # Only one argument, randomly split dataset
        data = sc.textFile(sys.argv[1]).map(parseLine)
        # random split
        train, test = data.randomSplit([0.8, 0.2], seed=0)
    else:
        sys.stderr.write('Provide at least one dataset for training.')
        sys.exit(1)

    model = NaiveBayes.train(train, 1.0)

    # Make prediction and test accuracy.
    predict_label = test.map(lambda p: (model.predict(p.features), p.label))
    accuracy = 1.0 * predict_label.filter(lambda (x, v): x == v).count() / test.count()

    print('Accuracy: %f' % accuracy)
