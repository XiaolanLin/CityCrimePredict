#!/usr/bin/evn python
import re
import sys
import matplotlib.pyplot as pyplot
import pandas as pandas

y_rate= list()

year_data = dict()

x_year = list()

# cat (each output file)  | sort | python resolvedRate_per_year.py

for line in sys.stdin:
    splits = line.split(',')
    second_splits = splits[1].split()
    pd_district = second_splits[0]
    year_data[int(splits[0])] = float(second_splits[1])

for key in year_data:
    x_year.append(key)
    y_rate.append(year_data[key])

df = pandas.DataFrame(y_rate, index = x_year, columns = [pd_district])

plot = df.plot(kind='bar', stacked=True)
plot.set_ylim(0,1)

pyplot.show()