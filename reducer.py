#!/usr/bin/python

# Import relevant module
import sys

# Initialize a value and an old key
total_confirmed = 0
old_key = None

# For every line in the standard input...
for line in sys.stdin:
    
    # ... strip and split it
    data = line.strip().split('\t')
    
    # ...if it has two values
    if len(data) == 2:
        
        # ...unpack them
        this_key, this_confirmed = data
        
        # ...and if the new key differs from the old key (and if the old key has been set previously)
        if old_key and old_key != this_key:
            
            # ...print the key-value pair
            print old_key, '\t', total_confirmed
            
            # ...reset the value
            total_confirmed = 0
        
        # ...update the key
        old_key = this_key
        
        # ...if this_confirmed is larger than the total_confirmed
        if int(this_confirmed) > total_confirmed:
            
            # ...set this_confirmed as the total_confirmed
            total_confirmed = int(this_confirmed)

# For the final line of the standard input...
if old_key != None:
    
    # ...print the key-value pair
    print old_key, '\t', total_confirmed
