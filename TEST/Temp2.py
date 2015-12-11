__author__ = 'Pandera'
import sys
sys.path.append('../common')
import json
import time
import configparser
from resumeResult import ResumeResult
from parsedWords import ParsedWord
from datetime import datetime
from pymongo import MongoClient
from dateutil import parser
from debugException import DebugException
from candidateClassifierIncreJob import candidate_classifier_incre
from rzindex_wrapper import rzindex_candidate
from candidateClassifierIncreJob import getCharacteristicMap

start_time = time.time()

#db.candidateresume_text.find( { $or: [ { "loaded_date": { $lte: createdDate } }, { "update_date": { $lte: createdDate } } ] } , \
#{"candidate_id": 1, "loaded_date": 1, "update_date": 1, "_id": 0} ).sort({"loaded_date": -1}).limit(1)

matchCount = 0
count = 0
total_count = 0
cursor_start = 0
fetch_limit = 100
WRITE_TO_DB = True
JOB_NAME = "resume_parser"

def obj_dict(self):
    return self.__dict__


def candResumeParser(resumeSkill):
    for resumeLine in resumeSkill:
        global matchCount
        wordcount = {}
        global count
        count += 1
        print("Running "+ str(count))
        #print(str(resumeLine))
        resumeText = resumeLine["resume_text"]
        #print("Text - %s" % resumeText)
        if resumeText is not None and resumeText is not "":
            candidateId = resumeLine["candidate_id"]
            resumeId = resumeLine["resume_id"]
            dataCenter = resumeLine["date_center"]
            currentResumeResult = ResumeResult(resumeId, candidateId, dataCenter)
            for interpreterLine in interpreterList:
                interpreterItem = " " + interpreterLine["Item"] + " "
                interpreterValue = interpreterLine["ItemType"]
                if interpreterItem is not None and interpreterItem is not "":
                    matchCount = resumeText.lower().count(interpreterItem.lower())
                    if matchCount > 0:
                        if interpreterValue != None:
                            currentResumeResult.parsedWords.append(
                                ParsedWord(interpreterItem.lower().strip(), matchCount, interpreterValue.lower()))
                        else:
                            currentResumeResult.parsedWords.append(
                                ParsedWord(interpreterItem.lower().strip(), matchCount, 0))

            for word in resumeText.lower().split():
                clean_word = word.strip()
                if clean_word not in wordcount:
                    wordcount[clean_word] = 1
                else:
                    wordcount[clean_word] += 1

            for word, match_count in wordcount.items():
                currentResumeResult.parsedWords.append(ParsedWord(word, match_count, 0))


            # print("---Parse Time: %s seconds ---" % (time.time() - start_time))
            resumeJSON = json.dumps(currentResumeResult, default=obj_dict)
            # print("---JSON Time: %s seconds ---" % (time.time() - start_time))
            mongoResume = json.loads(resumeJSON)
            # print("---BSON Time: %s seconds ---" % (time.time() - start_time))

            if WRITE_TO_DB:
                db.candidate_skills_from_parsed_resumes.update({"candidate_id": candidateId, "resume_id": resumeId},
                                                           mongoResume, upsert=True)
    return count


try:
    config = configparser.ConfigParser()
    config.read('../common/ConfigFile.properties')

    uri = config.get("DatabaseSection", "database.connection_string")
    db_name = config.get("DatabaseSection", "database.dbname")
    client=MongoClient(uri)

    db = client[db_name]
    #print("%s" % db)

    beginTime = datetime.utcnow()
    interpreterList = list(db.master_interpreter.find())
    jobs = list(db.etl_job_log.find({ "job_name" : JOB_NAME }).sort("start_datetime",  -1).limit(1))
    for j in jobs:
        job = j

    lastRunDate = job["start_datetime"]
    candidateIdList = []
    #resumeCandidateList = list(db.candidate_resume_text.find( { "$or": [ { "loaded_date": { "$gt": lastRunDate } }, { "update_date": { "$gt": lastRunDate } } ] },{"candidate_id" : 1, "_id" : 0} ))
    #for candidate in resumeCandidateList:
        #candidateIdList.append(candidate["candidate_id"])
    #candidateList = list(db.candidate.find( { "$or": [ { "loaded_date": { "$gt": lastRunDate } }, { "update_date": { "$gt": lastRunDate } } ] },{"candidate_id" : 1, "_id" : 0} ))
    #for candidate in candidateList:
        #candidateIdList.append(candidate["candidate_id"])
    candidateList= list(db.candidate_resume_text.find({ "$and": [ { "resume_text": { "$nin": [""] } }, { "resume_text": { "$nin": [None] } } ] }).distinct("candidate_id"))
    candidateList= list(db.candidate_resume_text.find({}).distinct("candidate_id"))
    canParsedList = list(db.candidate_skills_from_parsed_resumes.find({}).distinct("candidate_id"))

    canDiff = list(set(candidateList) - set(canParsedList))

    candidateIdList = list(set(canDiff))
    #total_resumes = db.candidate_resume_text.find( { "$or": [ { "loaded_date": { "$gt": lastRunDate } }, { "update_date": { "$gt": lastRunDate } } ] } ).count()
    #candidatetotal = len(candidateIdList)
    #candidateIdList = [117542,32040]
    candidatetotal = len(candidateIdList)
    print(str(candidatetotal))
    #print(candidateIdList)
    log_file = open("resumeParser.log", "a")
    etl_job_log = {}
    temp = []
    charcteristics_list = getCharacteristicMap()
    for i in range(0,candidatetotal):
        candidateResumeSkills = list(db.candidate_resume_text.find({"candidate_id":candidateIdList[i]}))
        print(str(candidateIdList[i]))
        #print(str(candidateResumeSkills))
        print("Parsing")
        temp = candResumeParser(candidateResumeSkills)
        print("Classifier")
        candidate_classifier_incre(candidateIdList[i],charcteristics_list)
        print("Scoring")
        rzindex_candidate(candidateIdList[i])

    #print("---Total Time: %s seconds ---" % (time.time() - start_time))
    etl_job_log["job_name"] = JOB_NAME
    etl_job_log["start_datetime"] = beginTime
    etl_job_log["end_datetime"] = datetime.utcnow()
    etl_job_log["elapsed_time_in_seconds"] = time.time() - start_time
    etl_job_log["total_records_processed"] = candidatetotal
    db.etl_job_log.insert_one(etl_job_log)
    log_file.write(str(etl_job_log))

except Exception as e:
    DebugException(e)
    log_file.write("Exception during resume parsing: [" + str(e) + "]")

log_file.close()
