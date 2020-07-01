import json
import os
import csv
import sys
import time

#set up a few variables from the user
ontfilename = sys.argv[1]

with open(ontfilename, "r+") as read_file:
    ont = json.load(read_file)

columns = [
	"inScheme",
	"prefLabel",
	"exactMatch",
	"closeMatch"
]

with open (ontfilename+".csv", "w+") as write_file:
    csvwriter = csv.writer(write_file)
    for concept in ont["@set"]:
        line = [
        	concept["@id"]
        ]
        for column in columns:
            try:
                line.append(concept[column].encode('utf-8'))
            except KeyError:
                line.append("-")
        csvwriter.writerow(line)

