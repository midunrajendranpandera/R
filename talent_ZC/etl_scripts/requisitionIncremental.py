import sys
sys.path.append('../common')
import json
import time
import configparser
from requisitionResult import RequisitionResult
from parsedWords import ParsedWord
from datetime import datetime
from pymongo import MongoClient
from debugException import DebugException
from requisitionIncrementalScorer import requisitionIncrementalScorer

begin_time = datetime.utcnow()
start_time = time.time()

cursor_start = 0
fetch_limit = 100
WRITE_TO_DB = True
JOB_NAME = "requisition_parser"

matchCount = 0
count = 0

def obj_dict(self):
    return self.__dict__


try:
    config = configparser.ConfigParser()
    config.read('../common/ConfigFile.properties')

    LOG_PATH = config.get("LogSection", "log.log_path")
    uri = config.get("DatabaseSection", "database.connection_string")
    db_name = config.get("DatabaseSection", "database.dbname")
    client = MongoClient(uri)
    db = client[db_name]

    s = datetime.today().strftime("%Y%m%d")
    logFileName = LOG_PATH + "requisitionParser_" + s + ".log"

    try:
        log_file = open(logFileName, "a")
    except (FileNotFoundError, IOError) as e:
        logFileName = "./requisitionParser_" + s + ".log"
        log_file = open(logFileName, "a")
        print("[" + datetime.now().isoformat() + "]  Configured LOG_PATH doesn't exist [" + str(e) + "]" )

    parsed_requisition = db["requisition_skills_from_parsed_requisition"]
    etlJob = db["etl_job_log"]

    beginTime = datetime.utcnow()
    interpreterList = list(db.master_interpreter.find())

    etl_job_log = {}
    total_req = db.requisition.find( { "etl_process_flag": False } ).count()
    log_file.write("[" + datetime.now().isoformat() + "] [" + JOB_NAME + "] [BEGIN]  Total requisitions to be processed [" + str(total_req) + "]" + "\n")

    while cursor_start < total_req:
        requisitionList = list(db.requisition.find( { "etl_process_flag": False }, {"requisition_id":1, "data_center" : 1, "req_requirements":1, "req_description":1, "requisition_text": 1}).skip(cursor_start).limit(fetch_limit))
        #requisitionList = list(db.requisition.find( {"requisition_id":{"$in": [113071]}},{"requisition_id":1, "data_center" : 1, "req_requirements":1, "req_description":1,"requisition_text": 1,"loaded_date":1,"update_date":1}))
        for requisition in requisitionList:
            reqBeginTime = datetime.now()
            count += 1
            wordcount = {}
            requisitionText = requisition["requisition_text"]
            if requisitionText is None:
                requisitionText = ""
            requisitionId = requisition["requisition_id"]
            #print("Running count [" + str(count) + "]  RequisitionId [" + str(requisitionId) + "]")
            dataCenter = requisition["data_center"]
            currentRequisitionResult = RequisitionResult(requisitionId, dataCenter)
            wc=[]
            for interpreterLine in interpreterList:
                interpreterItem = " " + interpreterLine["Item"] + " "
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

            reqJSON = json.dumps(currentRequisitionResult, default=obj_dict)
            mongoReq = json.loads(reqJSON)

            if WRITE_TO_DB:
                db.requisition_skills_from_parsed_requisition.update( { "requisition_id" : requisitionId }, mongoReq, upsert=True)

            status = requisitionIncrementalScorer(requisitionId, db, log_file)
            #After process completed, set the etl_process_flag to true indicating that the requisition incremental processing is complete
            db.requisition.update( {"requisition_id": requisitionId}, { "$set" : {"etl_process_flag" : True} } )
            log_file.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] total elapsed Ttime [" + str(datetime.now() - reqBeginTime) + "]" + "\n")
        cursor_start += fetch_limit


    log_file.write("[" + datetime.now().isoformat() + "] [" + JOB_NAME + "] [END]  Total Requisitions processed : [" + str(count) + "] Total time elapsed [" + str(time.time() - start_time) + "]" + "\n")

    etl_job_log = {}
    etl_job_log["job_name"] = JOB_NAME
    etl_job_log["start_datetime"] = begin_time
    etl_job_log["end_datetime"] = datetime.utcnow()
    etl_job_log["elapsed_time_in_seconds"] = time.time() - start_time
    etl_job_log["total_records_processed"] = count
    db.etl_job_log.insert_one(etl_job_log)

except Exception as e:
    DebugException(e)
    msg = "[requisitionParser]" + e
    log_file.write("[" + datetime.now().isoformat() + "] JOB_NAME [" + JOB_NAME + "] exception during processing [" + str(e) + "]" + "\n")

log_file.write("-----" + "\n")
log_file.close()

