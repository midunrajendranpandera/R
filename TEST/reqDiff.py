__author__ = 'Pandera'
import sys
sys.path.append('../common')
import json
import time
import configparser
import collections
import operator
import math
from datetime import datetime
from pymongo import MongoClient
from dateutil import parser
from collections import Counter
from debugException import DebugException

config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db = client[db_name]

requisitionList = list(db.requisition.find({"req_status_id" : {"$in":[2,3,6,9,10,11,12]}}).distinct("requisition_id"))
reqParsedList = list(db.requisition_skills_from_parsed_requisition.find({}).distinct("requisition_id"))
reqDiff = list(set(requisitionList) - set(reqParsedList))
reqParsedMissed = []
for requisition in reqDiff:
    reqText = list(db.requisition.find({"requisition_id" : requisition},{"requisition_text":1,"_id":0}))
    for reqtext in  reqText:
        text = reqtext["requisition_text"]
    if(len(text)>0):
        reqParsedMissed.append(requisition)

print("Number of reqs to be parsed %s", %len(reqParsedMissed))


