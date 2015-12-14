#!/usr/bin/evn python
import re
import sys
import matplotlib.pyplot as pyplot
import pandas as pandas

yaxis = list()

xaxis_data = dict()

weekday = list()

xaxis = list()

colors = ['#365C96']

# cat [file] | sort | python crime_per_hour_count.py

for line in sys.stdin:
	splits = line.split()
	print splits
	xaxis_data[splits[1]] = int(splits[2])
	weekday.append(splits[1])

for key in weekday:
	xaxis.append(key)
	yaxis.append(xaxis_data[key])

df = pandas.DataFrame(yaxis, index = xaxis)

df.plot(kind='bar', stacked=True, color = colors, title='Count of Incidents for each weekday', legend=False)
pyplot.xlabel('Weekday')
pyplot.ylabel('Count')
pyplot.show()


	