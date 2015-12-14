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
INDEX_ADDRESS = 8
INDEX_LONGITUDE = 9
INDEX_LATITUDE = 10

# Naive bayes model in Spark mllib require float for each feature and label.
# Define dictionary to map features/label to float numbers

# Label: Crime Category
Category = {
    'ASSAULT': 0,
    'VANDALISM': 1,
    'DISORDERLY CONDUCT': 2,
    'OTHER OFFENSES': 3,
    'FAMILY OFFENSES': 4,
    'ARSON': 5,
    'PORNOGRAPHY/OBSCENE MAT': 6,
    'PROSTITUTION': 7,
    'SEX OFFENSES, FORCIBLE': 8,
    'SEX OFFENSES, NON FORCIBLE': 9,
    'DRIVING UNDER THE INFLUENCE': 10,
    'DRUNKENNESS': 11,
    'DRUG/NARCOTIC': 12,
    'LIQUOR LAWS': 13,
    'LARCENY/THEFT': 14,
    'VEHICLE THEFT': 15,
    'ROBBERY': 16,
    'STOLEN PROPERTY': 17,
    'TRESPASS': 18,
    'EMBEZZLEMENT': 19,
    'BURGLARY': 20,
    'EXTORTION': 21,
    'FORGERY/COUNTERFEITING': 22,
    'FRAUD': 23,
    'BAD CHECKS': 24,
    'BRIBERY': 25,
    'GAMBLING': 26,
    'KIDNAPPING': 27,
    'MISSING PERSON': 28,
    'LOITERING': 29,
    'SUICIDE': 30,
    'RUNAWAY': 31,
    'SECONDARY CODES': 32,
    'RECOVERED VEHICLE': 33,
    'WEAPON LAWS': 34,
    'NON-CRIMINAL': 35,
    'SUSPICIOUS OCC': 36,
    'WARRANTS': 37,
    'TREA': 38,
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
