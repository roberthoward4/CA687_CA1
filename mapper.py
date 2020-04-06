#!/usr/bin/python

# Import relevant module
import sys

# For every line in the data...
for line in sys.stdin:
    
    # ...strip and split it
    data = line.strip().split(' ')
    
    # ...if it has seven values
    if len(data) == 7:
        
        # ...unpack them
        Country, Lat, Long, Date, Confirmed, Deaths, Recovered = data

        # ...print the key-value pair
        print '{0}\t{1}'.format(Country, Confirmed)
 