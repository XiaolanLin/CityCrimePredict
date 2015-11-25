#!/usr/bin/evn python
import re
import sys
import matplotlib.pyplot as pyplot
import pandas as pandas

yaxis = list()

hourly_data = dict()

xaxis = list()

# cat [file] | sort | python crime_per_hour_count.py

for line in sys.stdin:
	splits = map(int, line.split())
	hourly_data[splits[0]] = splits[1]

for key in hourly_data:
	xaxis.append(key)
	yaxis.append(hourly_data[key])

df = pandas.DataFrame(yaxis, index = xaxis)

df.plot(kind='bar', stacked=False, legend=False)
pyplot.xlabel('hour')
pyplot.ylabel('count')
pyplot.show()


	