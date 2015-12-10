import sys
sys.path.append('../common')
import json
import time
import configparser
from datetime import datetime
from pymongo import MongoClient
from dateutil import parser
from debugException import DebugException
#from rzindex_wrapper import rzindex_wrapper_insert
from requisitionIncrementalScorer import subReqCandScorer

start_time = time.time()

count = 0
cursor_start = 0
fetch_limit = 100
WRITE_TO_DB = True
JOB_NAME = "submitted_candidate_scoring"

def obj_dict(self):
    return self.__dict__

try:
    config = configparser.ConfigParser()
    config.read('../common/ConfigFile.properties')

    uri = config.get("DatabaseSection", "database.connection_string")
    db_name = config.get("DatabaseSection", "database.dbname")
    client=MongoClient(uri)
    #print(uri)
    db = client[db_name]
    #print("%s" % db)

    beginTime = datetime.utcnow()
    
    jobs = list(db.etl_job_log.find({ "job_name" : JOB_NAME }).sort("start_datetime",  -1).limit(1))
    for j in jobs:
        job = j

    lastRunDate = job["start_datetime"]
    print(lastRunDate)
    requisition_count = db.requisition_candidate.find( { "$or": [ { "loaded_date": { "$gt": lastRunDate } }, { "update_date": { "$gt": lastRunDate } } ] } ).count()
    print(str(requisition_count))
    #print(requisition_count)
    log_file = open("reqCandidateScoring.log", "a")
    #reqList = list(db.requisition_candidate.find({"requisition_id":1825 }, {"requisition_id" : 1, "_id" : 0} ).distinct("requisition_id"))   
    reqList = list(db.requisition_candidate.find( { "$or": [ { "loaded_date": { "$gt": lastRunDate } }, { "update_date": { "$gt": lastRunDate } } ] }, {"requisition_id" : 1, "_id" : 0} ).distinct("requisition_id"))
    #print(reqList)
    reqlen = len(reqList)
    for i in range(0,reqlen):
        #print(1)
        #print(reqList[i])
        reqId=int(reqList[i])
        status = subReqCandScorer(reqId,db)
        #candListCount = db.requisition_candidate.find( { "requisition_id" : reqId,  "$or": [ { "loaded_date": { "$gt": lastRunDate } }, { "update_date": { "$gt": lastRunDate } } ] }  ).count()
        #cursor_start = 0
        #while cursor_start < candListCount:
            #candList = list(db.requisition_candidate.find( { "requisition_id" : reqId,  "$or": [ { "loaded_date": { "$gt": lastRunDate } }, { "update_date": { "$gt": lastRunDate } } ] }, {"candidate_id" : 1, "_id" : 0} ).skip(cursor_start).limit(fetch_limit))
            #candidList = []
            #print(candidList)            
            #for cand in candList:
                #candidList.append(cand["candidate_id"])
            #print(reqId)
            #print(candidList)
            #rzindex_wrapper_insert(reqId,candidList)           
            #status = subReqCandScorer(reqId,db)
            #print(reqId)
            #print(candidList)	
            #cursor_start += fetch_limit

    #print("---Total Time: %s seconds ---" % (time.time() - start_time))
    job_log = {}
    job_log["job_name"] = JOB_NAME
    job_log["start_datetime"] = beginTime
    job_log["end_datetime"] = datetime.utcnow()
    job_log["elapsed_time_in_seconds"] = time.time() - start_time
    job_log["total_records_processed"] = requisition_count
    #jsonlog = json.dumps(job_log)
    log_file.write(str(job_log))
    db.etl_job_log.insert_one(job_log)

except Exception as e:
    DebugException(e)
    log_file.write("Exception during submitted Candidate Scoring: [" + str(e) + "]")

log_file.close()

