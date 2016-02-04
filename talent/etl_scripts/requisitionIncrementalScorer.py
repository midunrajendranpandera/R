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
#from dateutil import parser
from collections import Counter
from debugException import DebugException
from scoringMethods import reqScorer
from scoringMethods import scoreInserter
from scoringMethods import candidateScorer

#config = configparser.ConfigParser()
#config.read('../common/ConfigFile.properties')

#uri = config.get("DatabaseSection", "database.connection_string")
#db_name = config.get("DatabaseSection", "database.dbname")
#client=MongoClient(uri)
#db = client[db_name]

def requisitionIncrementalScorer(requisitionId, db, logFile):
    try:

        reqParsed = list(db.requisition_skills_from_parsed_requisition.find({"requisition_id":requisitionId},{"parsedWords":1,"_id":0}))
        for words in reqParsed:
            if(words["parsedWords"] == []):
                reqParsed = []
        if(len(reqParsed) == 0):
            status = scoreInserter(requisitionId)
            return("No Requisition description")
            logFile.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] No description" + "\n")

        ### - Submitted Candidates processing
        scBeginTime = datetime.now()
        candidateCount = db.requisition_candidate.find({"requisition_id":requisitionId}).count()
        jobid = list(db.requisition.find({"requisition_id": requisitionId}, {"new_global_job_category_id": 1, "_id": 0}))
        for id in jobid:
            gjid1 = id["new_global_job_category_id"]
        idealSkillList = db.ideal_candidate_characteritics.find_one({"global_job_category_id": gjid1}, {"Skills": 1, "_id": 0})
        if(idealSkillList is None):
            idealSkills = 'NA'
        else:
            idealSkills = idealSkillList['Skills']
        if(candidateCount >=1):
            #print("Working on Submitted Candidates")
            status = reqScorer(reqParsed, jobid, idealSkills, requisitionId)
            logFile.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] Submitted candidates scoring complete. Candidate Count ["+ str(candidateCount) + "] Elapsed time [" + str(datetime.now() - scBeginTime) + "]" + "\n")
        else:
            logFile.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] no submitted candidates found" + "\n")

        ## - Search & Score
        ssBeginTime = datetime.now()
        candidateList = []
        candidates = list(db.category_candidate_map.find({"global_job_category_id":gjid1}, {"candidates":1,"_id":0}))
        for candidate in candidates:
            candidateList = candidate["candidates"]
        candidateTotal = len(candidateList)
        if(candidateTotal == 0):
            logFile.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] No Candidates associated to this Job Category [" + str(gjid1) + "]" + "\n")
            #print("No Candidates associated to this Job Category")
            return("Scoring Method Completed Successfully")
        #print("Requisition %s has %s affiliated candidates" % (requisitionId, candidateTotal))
        status = candidateScorer(candidateList, reqParsed, idealSkills, requisitionId)
        logFile.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] Search & Score scoring complete. Candidate Count ["+ str(candidateTotal) + "] Elapsed time [" + str(datetime.now() - ssBeginTime) + "]" + "\n")
        return("Scoring Complete")
    except Exception as e:
        DebugException(e)
        logFile.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] [requisitionIncrementalScorer()] Requisition Incremental Scorer failed due to error [" + str(e) + "]" + "\n")

def subReqCandScorer(requisitionId, db, logFile):
    try:
        scBeginTime = datetime.now()
        reqParsed = list(db.requisition_skills_from_parsed_requisition.find({"requisition_id":requisitionId},{"parsedWords":1,"_id":0}))
        for words in reqParsed:
            if(words["parsedWords"] == []):
                reqParsed = []

        if(len(reqParsed) == 0):
            status = scoreInserter(requisitionId)
            logFile.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] No description" + "\n")
            return("No Requisition description")

        jobid = list(db.requisition.find({"requisition_id":requisitionId},{"new_global_job_category_id":1,"_id":0}))
        for id in jobid:
            gjid1 = id["new_global_job_category_id"]
        idealSkillList = db.ideal_candidate_characteritics.find_one({"global_job_category_id":gjid1},{"Skills":1,"_id":0})
        if(idealSkillList is None):
            idealSkills = 'NA'
        else:
            idealSkills = idealSkillList['Skills']
        status = reqScorer(reqParsed, jobid, idealSkills, requisitionId)
        logFile.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] Submitted candidates scoring complete. Elapsed time [" + str(datetime.now() - scBeginTime) + "]" + "\n")
        return("Scoring Complete")
    except Exception as e:
        DebugException(e)
        #print("Requisition Incremental Scorer failed due to error [" + str(e) + "]")
        logFile.write("[" + datetime.now().isoformat() + "] RequisitionId [" + str(requisitionId) + "] [subReqCandScorer()] Requisition Incremental Scorer failed due to error [" + str(e) + "]" + "\n")
