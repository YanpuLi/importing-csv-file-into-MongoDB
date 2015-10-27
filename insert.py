#!/usr/bin/env python
""" 
fileProcess.py:
The function of this file is to parse csv file into json format(array of documents)

insert.py:
This file is for inserting the documents into MongoDB

EXAMPLE:
python insert.py --db ** --col ** --path **

"""

from fileProcess import process_file
from pymongo import MongoClient
import glob
import argparse

parser = argparse.ArgumentParser(description='data uploading')
parser.add_argument('--db', action='store', dest='dbName', help='store the db name')
parser.add_argument('--col', action='store',dest='dbCollection',help='store the collection name')
parser.add_argument('--path', action='store',dest='filePath',help='store the file path')
result = parser.parse_args()
# connecting to DB
client = MongoClient("mongodb://localhost:27017")
db = client[result.dbName]

def insert_autos(infile, db, col):
    data = process_file(infile)
    db[col].insert(data)
    print "import:%i pieces of documents into MongoDB"%(len(data))
    # reading in all the csv files
for files in glob.glob("%s/*.csv"%result.filePath):
    insert_autos(files, db,result.dbCollection)
    print db[result.dbCollection].find_one()

  


