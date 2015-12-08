import csv

from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from numpy import array
import random
"""
export SPARK_HOME=/Users/yummin/Program/Java/lib/spark-1.5.1-bin-hadoop2.6
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH
"""

INDEX_CATEGORY = 1
INDEX_DESCRIPT = 2
INDEX_DAYOFWEEK = 3
INDEX_DATE = 4
INDEX_TIME = 5
INDEX_PD_DISTRICT = 6
INDEX_RESOLUTION = 7

PdDistrict = {'BAYVIEW': 1.0,
              'CENTRAL': 2.0,
              'INGLESIDE': 3.0,
              'MISSION': 4.0,
              'NORTHERN': 5.0,
              'PARK': 6.0,
              'RICHMOND': 7.0,
              'SOUTHERN': 8.0,
              'TARAVAL': 9.0,
              'TENDERLOIN': 10.0, }

DayOfWeek = {'Monday': 1.0,
             'Tuesday': 2.0,
             'Wednesday': 3.0,
             'Thursday': 4.0,
             'Friday': 5.0,
             'Saturday': 6.0,
             'Sunday': 7.0}

#according to https://en.wikipedia.org/wiki/White-collar_crime#Blue-collar_crime
#white color :0.0 blue color:1.0 other:2.0
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
    'SECONDARY CODES': 2.0
}

Description = {"GRAND THEFT FROM LOCKED AUTO": 0,
               "LOST PROPERTY": 1,
               "BATTERY": 2,
               "STOLEN AUTOMOBILE": 3,
               "DRIVERS LICENSE, SUSPENDED OR REVOKED": 4,
               "WARRANT ARREST": 5,
               "SUSPICIOUS OCCURRENCE": 6,
               "AIDED CASE, MENTAL DISTURBED": 7,
               "PETTY THEFT FROM LOCKED AUTO": 8,
               "MALICIOUS MISCHIEF, VANDALISM OF VEHICLES": 9,
               "TRAFFIC VIOLATION": 10,
               "PETTY THEFT OF PROPERTY": 11,
               "MALICIOUS MISCHIEF, VANDALISM": 12,
               "THREATS AGAINST LIFE": 13,
               "FOUND PROPERTY": 14,
               "ENROUTE TO OUTSIDE JURISDICTION": 15,
               "GRAND THEFT OF PROPERTY": 16,
               "POSSESSION OF NARCOTICS PARAPHERNALIA": 17,
               "PETTY THEFT FROM A BUILDING": 18}

Resolution = {'NONE': 0.0, 'ARREST, BOOKED': 1.0, 'ARREST, CITED': 2.0}

top1Hours = [18, 17, 12, 16, 19]
top2Hours = [15, 22, 0, 20, 14, 21, 13, 23]
top3Hours = [11, 10, 9, 8, 1]
top4Hours = [7, 2, 3, 6, 4, 5]


def parseLine(line):
    parts = list(csv.reader([line], delimiter=',', quotechar='"')).pop()
    label = Category[parts[INDEX_CATEGORY]]
    day = DayOfWeek[parts[INDEX_DAYOFWEEK]]
    month = getMonth(int(parts[INDEX_DATE].split('/')[1]))
    hour = getHourFeature(int(parts[INDEX_TIME].split(':')[0]))
    day_of_month = float(parts[INDEX_DATE].split('/')[0])
    year = getYear(int(parts[INDEX_DATE].split('/')[2]))
    pd = PdDistrict[parts[INDEX_PD_DISTRICT]]
    decript = getDescription(parts[INDEX_DESCRIPT])

    # latitude = getLatitude(abs(float(parts[9])))
    # longitude = getLongitude(float(parts[10]))
    # resolution = getResolution(parts[INDEX_RESOLUTION])

    # features = Vectors.dense([day, month, year, hour, day_of_month, pd, decript])
    features = Vectors.dense([day, month, year, hour, day_of_month, pd])
    return LabeledPoint(label, features)


def getYear(year):
    if year in [2013, 2014, 2003, 2004, 2005, 2008, 2012]:
        return 1.0
    elif year in [2009, 2006, 2007, 2010, 2011]:
        return 2.0
    elif year == 2015:
        return 3.0
    else:
        return 0.0


def getMonth(month):
    # classify the month by analysis result
    if month in [8, 3, 1, 5]:
        return 1.0
    elif month in [9, 7, 4, 10]:
        return 2.0
    elif month in [6, 2, 11, 12]:
        return 3.0
    return 0.0


def getDescription(descript):
    for v, p in Description.iteritems():
        if descript in v:
            return float(p)
    return 19.0


def getResolution(resolution):
    for v, p in Resolution.iteritems():
        if v == resolution:
            return p
    return 3.0


def getHourFeature(hour):
    if hour in top1Hours:
        return 1.0
    elif hour in top2Hours:
        return 2.0
    elif hour in top3Hours:
        return 3.0
    elif hour in top4Hours:
        return 4.0


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


if __name__ == "__main__":

    dataset = '/Users/Eva/MapReduce/testFiles/2014data.csv'

    testDataSet = '/Users/Eva/MapReduce/testFiles/201501test.csv'

    sc = SparkContext(appName="PythonNaiveBayesExample")

    # training test mannually split
    # training = sc.textFile(dataset).map(parseLine)

    # test = sc.textFile(testDataSet).map(parseLine)

    # random split
    data = sc.textFile('/Users/yummin/Downloads/sfpd-incident-2013-partial.csv').map(parseLine)

    training, test = data.randomSplit([0.8, 0.2], seed=0)

    model = NaiveBayes.train(training, 1.0)

    # Make prediction and test accuracy.
    prediction_label = test.map(lambda p: (model.predict(p.features), p.label))
    accuracy = 1.0 * prediction_label.filter(lambda (x, v): x == v).count(
    ) / test.count()

    print(accuracy)
