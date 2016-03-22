__author__ = 'Pandera'

import sys

sys.path.append('../common')
import re
import string
import pymongo
import json
import time
import configparser
from pprint import pprint
from classifierObject import ClassifierObject
from debugException import DebugException
from pymongo import MongoClient

config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")


client = MongoClient(uri)

db = client[db_name]

fileList = ["category_classifier_0.json",
            "category_classifier_1.json",
            "category_classifier_2.json",
            "category_classifier_3.json"]

def main():
    print("GO")
    category_map = db["category_candidate_map"]
    start_time = time.time()
    FirstFile = 0
    try:
        for f in fileList:
            print("Working on File[%s] - Time taken from start[%s]" % (f, (time.time() - start_time)))
            with open(f) as data_file:
                data = json.load(data_file)
                for d in data:
                    #print("Updating %s with %s candidates" % (d["global_job_category_id"], len(d["candidates"])))
                    category_map.update({"global_job_category_id": d["global_job_category_id"]}, {"$push": {"candidates": {"$each": d["candidates"]}}},upsert=True)
        print("---Total Time: %s seconds ---" % (time.time() - start_time))

    except Exception as e:
        DebugException(e)
        print("---Run Time: %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__": main()

print(fileList)

