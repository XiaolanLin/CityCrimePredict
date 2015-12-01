import csv

from pyspark import SparkContext
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

"""
export SPARK_HOME=/Users/yummin/Program/Java/lib/spark-1.5.1-bin-hadoop2.6
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH
"""

INDEX_CATEGORY = 1
INDEX_DAYOFWEEK = 3
INDEX_DATE = 4
INDEX_TIME = 5
INDEX_PD_DISTRICT = 6

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

Category = {'ARSON': 1.0,
            'ASSAULT': 2.0,
            'BAD CHECKS': 3.0,
            'BRIBERY': 4.0,
            'BURGLARY': 5.0,
            'DISORDERLY CONDUCT': 6.0,
            'DRIVING UNDER THE INFLUENCE': 7.0,
            'DRUG/NARCOTIC': 8.0,
            'DRUNKENNESS': 9.0,
            'EMBEZZLEMENT': 10.0,
            'EXTORTION': 11.0,
            'FAMILY OFFENSES': 12.0,
            'FORGERY/COUNTERFEITING': 13.0,
            'FRAUD': 14.0,
            'GAMBLING': 15.0,
            'KIDNAPPING': 16.0,
            'LARCENY/THEFT': 17.0,
            'LIQUOR LAWS': 18.0,
            'LOITERING': 19.0,
            'MISSING PERSON': 20.0,
            'NON-CRIMINAL': 21.0,
            'OTHER OFFENSES': 22.0,
            'PORNOGRAPHY/OBSCENE MAT': 23.0,
            'PROSTITUTION': 24.0,
            'ROBBERY': 25.0,
            'RUNAWAY': 26.0,
            'SECONDARY CODES': 27.0,
            'SEX OFFENSES, FORCIBLE': 28.0,
            'SEX OFFENSES, NON FORCIBLE': 29.0,
            'STOLEN PROPERTY': 30.0,
            'SUICIDE': 31.0,
            'SUSPICIOUS OCC': 32.0,
            'TRESPASS': 33.0,
            'VANDALISM': 34.0,
            'VEHICLE THEFT': 35.0,
            'WARRANTS': 36.0,
            'WEAPON LAWS': 37.0,
            'RECOVERED VEHICLE': 38.0}


def parseLine(line):
    parts = list(csv.reader([line], delimiter=',', quotechar='"')).pop()
    label = Category[parts[INDEX_CATEGORY]]
    day = DayOfWeek[parts[INDEX_DAYOFWEEK]]
    month = float(parts[INDEX_DATE].split('/')[1])
    hour = float(parts[INDEX_TIME].split(':')[0])
    pd = PdDistrict[parts[INDEX_PD_DISTRICT]]
    features = Vectors.dense([day, month, hour, pd])
    return LabeledPoint(label, features)


if __name__ == "__main__":

    dataset = '/Users/yummin/Downloads/sfpd-incident-2013-partial.csv'

    sc = SparkContext(appName="PythonNaiveBayesExample")

    # $example on$
    data = sc.textFile(dataset).map(parseLine)

    training, test = data.randomSplit([0.8, 0.2], seed=0)

    model = NaiveBayes.train(training, 1.0)

    # Make prediction and test accuracy.
    prediction_label = test.map(lambda p: (model.predict(p.features), p.label))
    accuracy = 1.0 * prediction_label.filter(lambda (x, v): x == v).count() / test.count()

    print(accuracy)
