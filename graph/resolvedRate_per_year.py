#!/usr/bin/evn python
import re
import sys
import matplotlib.pyplot as pyplot
import pandas as pandas


year_data = dict()

pdDistricts = dict()

dfs = dict()

district_List = ['BAYVIEW', 'CENTRAL', 'INGLESIDE', 'MISSION', 'NORTHERN', 'PARK', 'RICHMOND', 'SOUTHERN', 'TARAVAL', 'TENDERLOIN']

# cat output_resolved/part-r-0000* | sort | python resolvedRate_per_year.py

for line in sys.stdin:
    splits = line.split(',')
    second_splits = splits[1].split()
    pd_district = second_splits[0]
    year_data[(pd_district, int(splits[0]))] = float(second_splits[1])

for pd in district_List:
	pd_year_data = dict()
	for key in year_data:
		if key[0] == pd:
			pd_year_data[key[1]] = year_data[key]
	pdDistricts[pd] = pd_year_data

fig, axes = pyplot.subplots(2, 5, sharey=True, figsize=(25,6))
for i, pd in enumerate(district_List):
	y_rate = list()
	x_year = list()
	for key in pdDistricts[pd]:
		x_year.append(key)
		y_rate.append(pdDistricts[pd][key])
	axes[i/5][i%5].plot(x_year, y_rate)
	axes[i/5][i%5].set_title(pd)
	
pyplot.tight_layout()
pyplot.show()

