from __future__ import print_function
import os
import csv

from pyspark import SparkContext
# $example on$
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
# $example off$

os.environ["SPARK_HOME"] = "/Users/dengtiantong/PycharmProjects/criminal/lib/spark-1.5.1-bin-hadoop2.6"
os.environ["PYTHONPATH"] = "$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH"

INDEX_CATEGORY = 1
INDEX_DAYOFWEEK = 3
INDEX_DATE = 4
INDEX_TIME = 5
INDEX_PD_DISTRICT = 6

PdDistrict = {'BAYVIEW': 0.0,
              'CENTRAL': 1.0,
              'INGLESIDE': 2.0,
              'MISSION': 3.0,
              'NORTHERN': 4.0,
              'PARK': 5.0,
              'RICHMOND': 6.0,
              'SOUTHERN': 7.0,
              'TARAVAL': 8.0,
              'TENDERLOIN': 9.0, }

DayOfWeek = {'Monday': 0.0,
             'Tuesday': 1.0,
             'Wednesday': 2.0,
             'Thursday': 3.0,
             'Friday': 4.0,
             'Saturday': 5.0,
             'Sunday': 6.0}

Category = {
    'EXTORTION': 0.0,
    'FORGERY/COUNTERFEITING': 0.0,
    'FRAUD': 0.0,
    'BAD CHECKS': 0.0,
    'BRIBERY': 0.0,
    'EMBEZZLEMENT': 0.0,
    'SUSPICIOUS OCC': 0.0,
    'VANDALISM': 1.0,
    'ASSAULT': 1.0,
    'LARCENY/THEFT': 1.0,
    'STOLEN PROPERTY': 1.0,
    'ROBBERY': 1.0,
    'DRIVING UNDER THE INFLUENCE': 1.0,
    'DISORDERLY CONDUCT': 1.0,
    'ARSON': 1.0,
    'OTHER OFFENSES': 1.0,
    'TRESPASS': 1.0,
    'BURGLARY': 1.0,
    'KIDNAPPING': 1.0,
    'PROSTITUTION': 1.0,
    'LIQUOR LAWS': 1.0,
    'VEHICLE THEFT': 1.0,
    'RECOVERED VEHICLE': 1.0,
    'WEAPON LAWS': 2.0,
    'NON-CRIMINAL': 2.0,
    'WARRANTS': 2.0,
    'TREA': 2.0,
    'FAMILY OFFENSES': 2.0,
    'MISSING PERSON': 2.0,
    'PORNOGRAPHY/OBSCENE MAT': 2.0,
    'SEX OFFENSES, FORCIBLE': 2.0,
    'SEX OFFENSES, NON FORCIBLE': 2.0,
    'DRUNKENNESS': 2.0,
    'DRUG/NARCOTIC': 2.0,
    'GAMBLING': 2.0,
    'LOITERING': 2.0,
    'SUICIDE': 2.0,
    'RUNAWAY': 2.0,
    'SECONDARY CODES': 2.0}


def parseLine(line):

    parts = list(csv.reader([line], delimiter=',', quotechar='"')).pop()
    label = Category[parts[INDEX_CATEGORY]]
    day = DayOfWeek[parts[INDEX_DAYOFWEEK]]
    month = float(parts[INDEX_DATE].split('/')[0])
    year = float(parts[INDEX_DATE].split('/')[2])
    hour = float(parts[INDEX_TIME].split(':')[0])
    pd = PdDistrict[parts[INDEX_PD_DISTRICT]]
    latitude = abs(float(parts[9]))
    longitude = float(parts[10])
    features = Vectors.dense([day, month, year, hour, pd, latitude, longitude])
    return LabeledPoint(label, features)

def getLatitude(latitude):
    if latitude > 122.48 and latitude < 122.52:
        return 1.0
    elif latitude > 122.44 and latitude <= 122.48:
        return 2.0
    elif latitude > 122.40 and latitude <= 122.44:
        return 3.0
    else:
        return 4.0

def getLongitude(longitude):
    if longitude > 37.79 and longitude < 37.83:
        return 1.0
    elif longitude > 37.76 and longitude <= 37.79:
        return 2.0
    elif longitude > 37.73 and longitude <= 37.76:
        return 3.0
    else:
        return 4.0


top1Hours = [18, 17, 12, 16, 19]
top2Hours = [15, 22, 0, 20, 14, 21, 13, 23]
top3Hours = [11, 10, 9, 8, 1]
top4Hours = [7, 2, 3, 6, 4, 5]

def getHourFeature(hour):
    if hour in top1Hours:
        return 1.0
    elif hour in top2Hours:
        return 2.0
    elif hour  in top3Hours:
        return 3.0
    elif hour in top4Hours:
        return 4.0

def getMonth(month):
    # classify the month by analysis result
    if month in [8, 3, 1, 5]:
        return 1.0
    elif month in [9, 7, 4, 10]:
        return 2.0
    elif month in [6, 2, 11, 12]:
        return 3.0
    return 0.0

def getYear(year):
    if year in [2013, 2014, 2003, 2004, 2005, 2008, 2012]:
        return 1.0
    elif year in [2009, 2006, 2007, 2010, 2011]:
        return 2.0
    elif year == 2015:
        return 3.0
    else:
        return 0.0

if __name__ == "__main__":

    sc = SparkContext(appName="PythonDecisionTreeExample")

    dataset = '/Users/dengtiantong/PycharmProjects/criminal/dataset/2014data.csv'

    testDataSet = '/Users/dengtiantong/PycharmProjects/criminal/dataset/201501data.csv'
    # $example on$
    # Load and parse the data file into an RDD of LabeledPoint.
    trainingData = sc.textFile(dataset).map(parseLine)

    testData = sc.textFile(testDataSet).map(parseLine)

    # Split the data into training and test sets (30% held out for testing)
    #(trainingData, testData) = data.randomSplit([0.9, 0.1])

    # Train a DecisionTree model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = DecisionTree.trainClassifier(trainingData, numClasses=3, categoricalFeaturesInfo={4:10},
                                         impurity='gini', maxDepth=7, maxBins=100)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('accuracy = ' + str(1-testErr))
    print('Learned classification tree model:')
    print(model.toDebugString())

