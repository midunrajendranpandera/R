import sys
sys.path.append('../common')
import json
import time
import configparser
from requisitionResult import RequisitionResult
from parsedWords import ParsedWord
from datetime import datetime
from pymongo import MongoClient
from dateutil import parser
from debugException import DebugException

start_time = time.time()

cursor_start = 0
fetch_limit = 100
WRITE_TO_DB = True
JOB_NAME = "requisition_parser"

matchCount = 0
count = 0

def obj_dict(self):
    return self.__dict__

log_file = open("requisitionParser.log", "a")

try:
    config = configparser.ConfigParser()
    config.read('../common/ConfigFile.properties')

    uri = config.get("DatabaseSection", "database.connection_string")
    db_name = config.get("DatabaseSection", "database.dbname")
    client = MongoClient(uri)
    db = client[db_name]
    #print("%s" % db)
except Exception as e:
    print("Exception while reading config file / acquiring DB connection [") + str(e) + "]")


try:

    parsed_requisition = db["requisition_skills_from_parsed_requisition"]
    etlJob = db["etl_job_log"]

    beginTime = datetime.now()
    interpreterList = list(db.master_interpreter.find())
    jobs = list(db.etl_job_log.find({ "job_name" : JOB_NAME }).sort("start_datetime",  -1).limit(1))
    for j in jobs:
        job = j

    print(job)
    lastRunDate = job["start_datetime"]
    print(lastRunDate)

    etl_job_log = {}

    total_req = db.requisition.find( { "$or": [ { "loaded_date": { "$gt": lastRunDate } }, { "update_date": { "$gt": lastRunDate } } ] } ).count()
    print("Total requisitions found: [" + str(total_req) + "]")

    while cursor_start < total_req:
        requisitionList = list(db.requisition.find( { "$or": [ { "loaded_date": { "$gt": lastRunDate } }, { "update_date": { "$gt": lastRunDate } } ] } , {"requisition_id":1, "data_center" : 1, "req_requirements":1, "req_description":1, "requisition_text": 1}).skip(cursor_start).limit(fetch_limit))
        for requisitionLine in requisitionList:
            wordcount = {}
            count += 1
            #print("Running "+ str(count))
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
                        currentRequisitionResult.parsedWords.append(ParsedWord(interpreterItem.lower(), matchCount, interpreterValue.lower()))
                    else:
                        currentRequisitionResult.parsedWords.append(ParsedWord(interpreterItem.lower(), matchCount, 0))

            for word in requisitionText.lower().split():
                clean_word = word.strip()
                if clean_word not in wordcount:
                    wordcount[clean_word] = 1
                else:
                    wordcount[clean_word] += 1

            for word,match_count in wordcount.items():
                currentRequisitionResult.parsedWords.append(ParsedWord(word, match_count, 0))

            #print("---Parse Time: %s seconds ---" % (time.time() - start_time))
            reqJSON = json.dumps(currentRequisitionResult, default=obj_dict)
            mongoReq = json.loads(reqJSON)

            if WRITE_TO_DB:
                db.requisition_skills_from_parsed_requisition.update( { "requisition_id" : requisitionId }, mongoReq, upsert=True)

            #print("---JSON Time: %s seconds ---" % (time.time() - start_time))

        cursor_start += fetch_limit

    print("---Total Time: %s seconds ---" % (time.time() - start_time))
    etl_job_log = {}
    etl_job_log["job_name"] = JOB_NAME
    etl_job_log["start_datetime"] = beginTime
    etl_job_log["end_date_time"] = datetime.now()
    etl_job_log["elapsed_time_in_seconds"] = time.time() - start_time
    etl_job_log["total_records_processed"] = count
    db.etl_job_log.insert_one(etl_job_log)

except Exception as e:
    DebugException(e)
    msg = "[requisitionParser]" + e

log_file.close()
