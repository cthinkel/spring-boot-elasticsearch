import csv
import json
import os

# output = open('test.json', 'w')

index = 0;
filename = 'f_5500_2017_latest' + str(index) + '.json'

# output.write('[')
with open('f_5500_2017_latest.csv') as f:
    reader = csv.DictReader(f)

    for row in reader:
    	output = open(filename, 'a+')
    	if output.tell() >= 10000000: # 10 mb
    		index = index + 1
    		filename = 'f_5500_2017_latest' + str(index) + '.json'
    	output.write('{ "index" : { "_index" : "filings", "_type" : "filing" }}')
    	output.write('\n')
    	json.dump(row, output)
    	output.write('\n')
    	# rows = list(row)
    	# with open('f_5500_2017_latest.json', 'w') as f:
    	# 	json.dump(rows, f)
# output.write(']')