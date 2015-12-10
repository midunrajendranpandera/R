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
from scoringMethods import reqScorer
from scoringMethods import scoreInserter
from scoringMethods import candidateScorer

#config = configparser.ConfigParser()
#config.read('../common/ConfigFile.properties')
#HISTORY_MATCH_NOISE = 0.7

#uri = config.get("DatabaseSection", "database.connection_string")
#db_name = config.get("DatabaseSection", "database.dbname")
#client=MongoClient(uri)
#db = client[db_name]

def requisitionIncrementalScorer(requisitionId,db):
    try:

        reqParsed = list(db.requisition_skills_from_parsed_requisition.find({"requisition_id":requisitionId},{"parsedWords":1,"_id":0}))
        for words in reqParsed:
            if(words["parsedWords"] == []):
                reqParsed = []
        if(len(reqParsed) == 0):
            status = scoreInserter(requisitionId)
            return("No Requisition description")
        ### - Working on Submitted Candidates
        candidateCount = db.requisition_candidate.find({"requisition_id":requisitionId}).count()
        jobid = list(db.requisition.find({"requisition_id":requisitionId},{"global_job_category_id":1,"_id":0}))
        for id in jobid:
            gjid1 = id["global_job_category_id"]
        idealSkillList = db.ideal_candidate_characteritics.find_one({"global_job_category_id":gjid1},{"Skills":1,"_id":0})
        idealSkills = idealSkillList['Skills']
        if(candidateCount >=1):
            print("Working on Submitted Candidates")
            status = reqScorer(reqParsed,jobid,idealSkills,requisitionId)
        else:
            print("No Submitted Candidates")

        ## - Search & Score
        print("Working on Search & Score Candidates")
        
        candidates = list(db.category_candidate_map.find({"global_job_category_id":gjid1},{"candidates":1,"_id":0}))
        for candidate in candidates:
            candidateId = candidate["candidates"]
        candidateTotal = len(candidateId)
        print("Requisition %s has %s affiliated candidates" % (requisitionId,candidateTotal))
        status = candidateScorer(candidateId,reqParsed,idealSkills,requisitionId)
        return("Scoring Complete")
    except Exception as e:
        DebugException(e)
        print("Requisition Incremental Scorer failed due to error [" + str(e) + "]")

def subReqCandScorer(requisitionId,db):
    try:
        reqParsed = list(db.requisition_skills_from_parsed_requisition.find({"requisition_id":requisitionId},{"parsedWords":1,"_id":0}))
        for words in reqParsed:
            if(words["parsedWords"] == []):
                reqParsed = []
        if(len(reqParsed) == 0):
            status = scoreInserter(requisitionId)
            return("No Requisition description")
        jobid = list(db.requisition.find({"requisition_id":requisitionId},{"global_job_category_id":1,"_id":0}))
        for id in jobid:
            gjid1 = id["global_job_category_id"]
        idealSkillList = db.ideal_candidate_characteritics.find_one({"global_job_category_id":gjid1},{"Skills":1,"_id":0})
        idealSkills = idealSkillList['Skills']
        status = reqScorer(reqParsed,jobid,idealSkills,requisitionId)
        return("Scoring Complete")
    except Exception as e:
        DebugException(e)
        print("Requisition Incremental Scorer failed due to error [" + str(e) + "]")
