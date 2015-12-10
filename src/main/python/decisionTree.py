from __future__ import print_function
import csv
import os
import sys

from pyspark import SparkContext
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from config import *


def parseLine(line):
    parts = list(csv.reader([line], delimiter=',', quotechar='"')).pop()
    # target label
    label = Category[parts[INDEX_CATEGORY]]
    # categorical features
    # year = parts[INDEX_DATE].split('/')[2]
    day = DayOfWeek[parts[INDEX_DAYOFWEEK]]
    month = int(parts[INDEX_DATE].split('/')[0]) - 1
    hour = int(parts[INDEX_TIME].split(':')[0])
    day_of_month = int(parts[INDEX_DATE].split('/')[1]) - 1
    pd = PdDistrict[parts[INDEX_PD_DISTRICT]]
    # continuous features
    latitude = abs(float(parts[9]))
    longitude = float(parts[10])

    features = Vectors.dense([day, month, hour, day_of_month, pd, latitude,
                              longitude])
    return LabeledPoint(label, features)


if __name__ == '__main__':

    sc = SparkContext(appName='DecisionTreePridiction')

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

    # Train a DecisionTree model.
    # Empty categoricalFeaturesInfo indicates all features are continuous.
    model = DecisionTree.trainClassifier(train,
                                         numClasses=6,
                                         categoricalFeaturesInfo={0: 7,
                                                                  1: 12,
                                                                  2: 24,
                                                                  3: 31,
                                                                  4: 10},
                                         impurity='gini',
                                         maxDepth=7,
                                         maxBins=100)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(test.map(lambda x: x.features))
    labelsAndPredictions = test.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(test.count())
    print('accuracy = ' + str(1 - testErr))
    print('Learned classification tree model:')
    print(model.toDebugString())
