#!/usr/bin/evn python
import re
import sys
import matplotlib.pyplot as pyplot
import pandas as pandas

yaxis = list()

xaxis_data = dict()

xaxis = list()

colors = ['#365C96']

# cat [file] | sort | python crime_per_hour_count.py

for line in sys.stdin:
	splits = map(int, line.split())
	xaxis_data[splits[0]] = splits[1]

for key in xaxis_data:
	xaxis.append(key)
	yaxis.append(xaxis_data[key])

df = pandas.DataFrame(yaxis, index = xaxis)

df.plot(kind='bar', stacked=True, color = colors, title='Count of Incidents for each month', legend=False)
pyplot.xlabel('Month')
pyplot.ylabel('Count')
pyplot.show()


	