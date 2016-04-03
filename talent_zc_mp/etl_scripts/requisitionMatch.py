import sys
sys.path.append('./common')
import re
import string
import pymongo
import json
import time
import configparser
from requisitionResult import RequisitionResult
from parsedWords import ParsedWord

start_time = time.time()

from pymongo import MongoClient

config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)

db = client[db_name]

reqs = db.test_requisition_skills_list_parsed_bulk_test

finalUpload = []
matchCount = 0
count = 0
cursor_start = 0
QUERY_LIMIT = 10

text_file = open("reqoutput.json", "w")
text_file.write("[")
interpreterList = list(db.master_interpreter.find())
total_req = db.requisition.count()
#total_req = 3500
print(str(total_req))
word_count_list = []

def obj_dict(self):
    return self.__dict__

while cursor_start < total_req:
    requisitionList = list(db.requisition.find({}, {"requisition_id":1, "data_center" : 1, "req_requirements":1, "req_description":1, "requisition_text": 1}).skip(cursor_start).limit(3500))
    for requisitionLine in requisitionList:
        wordcount = {}
        count += 1
        print("Running "+ str(count))
        requisitionText = requisitionLine["requisition_text"]
        if requisitionText is None:
            requisitionText = ""
        requisitionId = requisitionLine["requisition_id"]
        dataCenter = requisitionLine["data_center"]
        currentRequisitionResult = RequisitionResult(requisitionId, dataCenter)
        wc=[]
        for interpreterLine in interpreterList:
            interpreterItem = " "+interpreterLine["Item"]+" "
            interpreterValue = interpreterLine["ItemType"]
            matchCount = requisitionText.lower().count(interpreterItem.lower())
            if matchCount > 0:            
                if interpreterValue != None:
                    currentRequisitionResult.parsedWords.append(ParsedWord(interpreterItem.lower().strip(), matchCount, interpreterValue.lower()))
                else:
                    currentRequisitionResult.parsedWords.append(ParsedWord(interpreterItem.lower().strip(), matchCount, 0))

        for word in requisitionText.lower().split():
            clean_word = word.strip()
            if clean_word not in wordcount:
                wordcount[clean_word] = 1
            else:
                wordcount[clean_word] += 1

        for word,match_count in wordcount.items():
            currentRequisitionResult.parsedWords.append(ParsedWord(word, match_count, 0))

        print("---Parse Time: %s seconds ---" % (time.time() - start_time)) 
        reqJSON = json.dumps(currentRequisitionResult, default=obj_dict)
        text_file.write("%s" % reqJSON)
        if count < total_req:
            text_file.write(",")

        print("---JSON Time: %s seconds ---" % (time.time() - start_time))

    cursor_start += 3500
text_file.write("]")
text_file.close()
print("---Total Time: %s seconds ---" % (time.time() - start_time))
