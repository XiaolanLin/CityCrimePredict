#!/usr/bin/env python

import re
import sys

import matplotlib.pyplot as plt
import pandas as pd

"""
Read line from stdin and plot bar diagrams

For PdDistrict or Category, run command like:
cat output_pd/part-r-0000* | sort | python plot_attrs_count.py
"""


# List to save all records
data = list()
# Dict to sava records for each year
each_year_data = dict()
# List to save years
years = list()

# Read each line from stdin
for line in sys.stdin:
    # Extract values using Regular Expression
    match = re.search(r'^\((\d{4}),\s([A-Z\s,/\-*]+)\)\s(\d+)$', line.strip())
    year = match.group(1)
    attr = match.group(2)
    count = match.group(3)

    if attr == '*':
        # each_year_data is not empty
        if each_year_data:
            # sanity check
            assert (total == sum(each_year_data.values()))
            # create a new dict for this year
            data.append(each_year_data)
            each_year_data = dict()

        total = int(count)
        years.append(year)
    else:
        each_year_data[attr] = int(count)
else:
    # sanity check
    assert (total == sum(each_year_data.values()))
    # create a new dict for this year
    data.append(each_year_data)

# Convert data to Pandas data frame
df = pd.DataFrame(data, index=years)

df.plot(kind='bar', stacked=True)
plt.show()
