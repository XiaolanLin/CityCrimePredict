#!/usr/bin/env python

from collections import Counter
import csv
import re
import sys

if len(sys.argv) == 2:
    dataset = sys.argv[1]
else:
    sys.stderr.write('Provide only one dataset.')
    sys.exit(1)

# Regular expression for street name
# If address has two streets, extract the first one
regexp = r'\s?([A-Z\d ]+) [A-Z]{2}'
# List to store street name
street = list()

# Read dataset
with open(dataset, 'rb') as f:
    reader = csv.reader(f, delimiter=',', quotechar='"')
    for row in reader:
        match = re.search(regexp, row[8])
        if match:
            street.append(match.group(1))
        else:
            sys.stderr.write('[WARN] %s can not be recognized.' % row[8])

# Count frequencies
counter = dict(Counter(street))

print('Found %d indecidents' % len(street))
print('Found %d unique streets' % len(counter))
for street in sorted(counter, key=counter.get, reverse=True):
    print('%s: %d' % (street, counter[street]))
