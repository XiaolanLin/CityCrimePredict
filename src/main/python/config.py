"""
Constants Declaration
"""
# Index of columns
INDEX_CATEGORY = 1
INDEX_DESCRIPT = 2
INDEX_DAYOFWEEK = 3
INDEX_DATE = 4
INDEX_TIME = 5
INDEX_PD_DISTRICT = 6
INDEX_RESOLUTION = 7

# Naive bayes model in Spark mllib require float for each feature and label.
# Define dictionary to map features/label to float numbers

# Label
# We defined 6 crime categories

Category = {
    # VANDALISM/ASSAULT:
    'ASSAULT': 1.0,
    'VANDALISM': 1.0,
    'DISORDERLY CONDUCT': 1.0,
    'OTHER OFFENSES': 1.0,
    'FAMILY OFFENSES': 1.0,
    'ARSON': 1.0,
    # Sexual crime:
    'PORNOGRAPHY/OBSCENE MAT': 2.0,
    'PROSTITUTION': 2.0,
    'SEX OFFENSES, FORCIBLE': 2.0,
    'SEX OFFENSES, NON FORCIBLE': 2.0,
    # Alcohol/Drug:
    'DRIVING UNDER THE INFLUENCE': 3.0,
    'DRUNKENNESS': 3.0,
    'DRUG/NARCOTIC': 3.0,
    'LIQUOR LAWS': 3.0,
    # THEFT/TRESPASS/FRAUD:
    'LARCENY/THEFT': 4.0,
    'VEHICLE THEFT': 4.0,
    'ROBBERY': 4.0,
    'STOLEN PROPERTY': 4.0,
    'TRESPASS': 4.0,
    'EMBEZZLEMENT': 4.0,
    'BURGLARY': 4.0,
    'EXTORTION': 4.0,
    # FORGERY
    'FORGERY/COUNTERFEITING': 5.0,
    'FRAUD': 5.0,
    'BAD CHECKS': 5.0,
    'BRIBERY': 5.0,
    'GAMBLING': 5.0,
    # OTHERS:
    'KIDNAPPING': 0.0,
    'MISSING PERSON': 0.0,
    'LOITERING': 0.0,
    'SUICIDE': 0.0,
    'RUNAWAY': 0.0,
    'SECONDARY CODES': 0.0,
    'RECOVERED VEHICLE': 0.0,
    'WEAPON LAWS': 0.0,
    'NON-CRIMINAL': 0.0,
    'SUSPICIOUS OCC': 0.0,
    'WARRANTS': 0.0,
    'TREA': 0.0,
}

# Police Department District
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

# Day of Week
DayOfWeek = {'Monday': 1.0,
             'Tuesday': 2.0,
             'Wednesday': 3.0,
             'Thursday': 4.0,
             'Friday': 5.0,
             'Saturday': 6.0,
             'Sunday': 0.0, }


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


def getHourFeature(hour):
    if hour in [18, 17, 12, 16, 19]:
        return 1.0
    elif hour in [15, 22, 0, 20, 14, 21, 13, 23]:
        return 2.0
    elif hour in [11, 10, 9, 8, 1]:
        return 3.0
    elif hour in [7, 2, 3, 6, 4, 5]:
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
