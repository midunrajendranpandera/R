import sys
sys.path.append('../common')
import json
import time
import configparser
from resumeResult import ResumeResult
from parsedWords import ParsedWord
from datetime import datetime
from pymongo import MongoClient
from debugException import DebugException
from candidateClassifierIncreJob import candidate_classifier_incre
from candidateClassifierIncreJob import getCharacteristicMap
#from candidateScoringIncremental import candidateIncrementalScorer

start_time = datetime.now()

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
        #count += 1
        #print("Running "+ str(count))
        #print(str(resumeLine))
        resumeText = resumeLine["resume_text"]
        #print("Text - %s" % resumeText)
        if resumeText is not None and resumeText is not "":
            candidateId = resumeLine["candidate_id"]
            #print(candidateId)
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

            ##Append the Candidate Skills to Resume
            candidateSkill = []
            candidateSkill = list(db.candidate.find({"candidate_id":candidateId}, {"candidate_id":1,"job_skill_names": 1,"_id":0}))
            for skillList in candidateSkill:
                for skills in skillList["job_skill_names"]:
                    skillTemp = {}
                    skillTemp["count"] = 1
                    skillTemp["word"] = skills["job_skill_name"].lower().strip()
                    skillTemp["interpreter_value"] = 'skills'
                    #print(skillTemp)
                    currentResumeResult.parsedWords.append(skillTemp)
            resumeJSON = json.dumps(currentResumeResult, default=obj_dict)
            mongoResume = json.loads(resumeJSON)

            if WRITE_TO_DB:
                db.candidate_skills_from_parsed_resumes.update({"candidate_id": candidateId, "resume_id": resumeId},
                                                           mongoResume, upsert=True)
    return count


try:
    config = configparser.ConfigParser()
    config.read('../common/ConfigFile.properties')

    LOG_PATH = config.get("LogSection", "log.log_path")
    uri = config.get("DatabaseSection", "database.connection_string")
    db_name = config.get("DatabaseSection", "database.dbname")
    client = MongoClient(uri)
    db = client[db_name]

    start_time = datetime.now()

    s = datetime.today().strftime("%Y%m%d")
    logFileName = LOG_PATH + "candidateIncremental_" + s + ".log"

    try:
        log_file = open(logFileName, "a")
    except (FileNotFoundError, IOError) as e:
        logFileName = "./candidateIncremental_" + s + ".log"
        log_file = open(logFileName, "a")
        print("[" + datetime.now().isoformat() + "]  Configured LOG_PATH doesn't exist [" + str(e) + "]" )


    log_file.write("[" + datetime.now().isoformat() + "] [" + JOB_NAME + "] [BEGIN]" + "\n")

    beginTime = datetime.utcnow()
    interpreterList = list(db.master_interpreter.find())

    candidateIdList = []
    resumeCandidateList = list(db.candidate_resume_text.find( { "etl_process_flag": False }, {"candidate_id" : 1, "_id" : 0} ))
    for candidate in resumeCandidateList:
        candidateIdList.append(candidate["candidate_id"])

    candidateList = list(db.candidate.find( { "etl_process_flag": False }, {"candidate_id" : 1, "_id" : 0} ) )
    for candidate in candidateList:
        candidateIdList.append(candidate["candidate_id"])
    candidateIdList = list(set(candidateIdList))
    #candidateIdList = [117542,32040]
    candidatetotal = len(candidateIdList)
    #print(str(candidatetotal))
    #print(candidateIdList)

    etl_job_log = {}
    temp = []	
    charcteristics_list = getCharacteristicMap()
    for i in range(0, candidatetotal):
        candBeginTime = datetime.now()
        candidateResumeSkills = list( db.candidate_resume_text.find({"candidate_id": candidateIdList[i]}) )
        temp = candResumeParser(candidateResumeSkills)
        log_file.write("[" + datetime.now().isoformat() + "] CandidateId [" + str(candidateIdList[i]) + "] Resume parsing elapsed time [" + str(datetime.now() - candBeginTime) + "]" + "\n")

        classiferBeginTime = datetime.now()
        candidate_classifier_incre(candidateIdList[i], charcteristics_list)
        log_file.write("[" + datetime.now().isoformat() + "] CandidateId [" + str(candidateIdList[i]) + "] classification elapsed time [" + str(datetime.now() - classiferBeginTime) + "]" + "\n")

        #scoreBeginTime = datetime.now()
        #candidateIncrementalScorer(candidateIdList[i], db)
        #log_file.write("[" + datetime.now().isoformat() + "] CandidateId [" + str(candidateIdList[i]) + "] scoring elapsed time [" + str(datetime.now() - scoreBeginTime) + "]" + "\n")

        count += 1
        db.candidate_resume_text.update( {"candidate_id": candidateIdList[i]}, { "$set" : {"etl_process_flag" : True} } )
        db.candidate.update( {"candidate_id": candidateIdList[i]}, { "$set" : {"etl_process_flag" : True} } )
        log_file.write("[" + datetime.now().isoformat() + "] CandidateId [" + str(candidateIdList[i]) + "] Total elapsed time [" + str(datetime.now() - candBeginTime) + "]" + "\n")

    log_file.write("[" + datetime.now().isoformat() + "] [" + JOB_NAME + "] [END]  Total candidates processed : [" + str(count) + "] Total time elapsed [" + str(datetime.now() - start_time) + "]" + "\n")
    etl_job_log["job_name"] = JOB_NAME
    etl_job_log["start_datetime"] = beginTime
    etl_job_log["end_datetime"] = datetime.utcnow()
    etl_job_log["elapsed_time_in_seconds"] = str(datetime.now() - start_time)
    etl_job_log["total_records_processed"] = candidatetotal
    db.etl_job_log.insert_one(etl_job_log)

except Exception as e:
    DebugException(e)
    log_file.write("[" + datetime.now().isoformat() + "] JOB_NAME [" + JOB_NAME + "] exception during processing [" + str(e) + "]" + "\n")

log_file.write("-----" + "\n")
log_file.close()
