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

    LOG_PATH = config.get("LogSection", "log.log_path")
    uri = config.get("DatabaseSection", "database.connection_string")
    db_name = config.get("DatabaseSection", "database.dbname")
    client=MongoClient(uri)
    db = client[db_name]

    s = datetime.today().strftime("%Y%m%d")
    logFileName = LOG_PATH + "reqCandidateScoring_" + s + ".log"

    try:
        log_file = open(logFileName, "a")
    except (FileNotFoundError, IOError) as e:
        logFileName = "./reqCandidateScoring_" + s + ".log"
        log_file = open(logFileName, "a")
        print("[" + datetime.now().isoformat() + "]  Configured LOG_PATH doesn't exist [" + str(e) + "]" )


    beginTime = datetime.utcnow()
    log_file.write("[" + datetime.now().isoformat() + "] [" + JOB_NAME + "] [BEGIN]" + "\n")

    requisition_count = db.requisition_candidate.find( { "etl_process_flag": False } ).count()
    #print(str(requisition_count))
    #reqList = list(db.requisition_candidate.find({"requisition_id":1825 }, {"requisition_id" : 1, "_id" : 0} ).distinct("requisition_id"))
    reqList = list(db.requisition_candidate.find( { "etl_process_flag": False }, {"requisition_id" : 1, "_id" : 0} ).distinct("requisition_id"))
    #print(reqList)
    reqlen = len(reqList)
    for i in range(0, reqlen):
        #print(reqList[i])
        reqBeginTime = datetime.now()
        reqId=int(reqList[i])
        status = subReqCandScorer(reqId, db, log_file)
        db.requisition_candidate.update( {"requisition_id": reqId,"etl_process_flag" : False }, { "$set" : {"etl_process_flag" : True}}, upsert=False, multi=True )
        count += 1
        log_file.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(reqId) + "] ElapsedTime [" + str(datetime.now() - reqBeginTime) + "]" + "\n")

    log_file.write("[" + datetime.now().isoformat() + "] [" + JOB_NAME + "] [END]  Total Requisitions processed : [" + str(count) + "] Total time elapsed [" + str(time.time() - start_time) + "]" + "\n")

    job_log = {}
    job_log["job_name"] = JOB_NAME
    job_log["start_datetime"] = beginTime
    job_log["end_datetime"] = datetime.utcnow()
    job_log["elapsed_time_in_seconds"] = time.time() - start_time
    job_log["total_records_processed"] = requisition_count
    db.etl_job_log.insert_one(job_log)

except Exception as e:
    DebugException(e)
    log_file.write("[" + datetime.now().isoformat() + "] JOB_NAME [" + JOB_NAME + "] exception during processing [" + str(e) + "]" + "\n")


log_file.write("-----" + "\n")
log_file.close()

