__author__ = 'Pandera'
import sys
sys.path.append('../common')
#import json
#import time
#import configparser
#import collections
#import operator
#import math
#from pymongo import MongoClient
#from dateutil import parser
#from collections import Counter
from datetime import datetime
from debugException import DebugException
from scoringMethods import reqScorer
from scoringMethods import scoreInserter
from scoringMethods import candidateScorer

statusID = [2,3,4,6,9,10,11,12]

def candidateIncrementalScorer(candidateId, db):
    #print(str(candidateId))
    candIdList = [candidateId]
    try:
        ##Working on Submitted requisitions
        requisitionList = list(db.requisition_candidate.find({"candidate_id":candidateId}).distinct("requisition_id"))
        if(len(requisitionList) > 0):
            for requisition in requisitionList:
                reqParsed = list(db.requisition_skills_from_parsed_requisition.find({"requisition_id":requisition},{"parsedWords":1,"_id":0}))
                for words in reqParsed:
                    if(words["parsedWords"] == []):
                        reqParsed = []
                if(len(reqParsed) == 0):
                    status = scoreInserter(requisition)
                    continue

                jobid = list(db.requisition.find({"requisition_id":requisition},{"new_global_job_category_id":1,"_id":0}))
                for id in jobid:
                    gjid1 = id["new_global_job_category_id"]
                idealSkillList = db.ideal_candidate_characteritics.find_one({"global_job_category_id":gjid1},{"Skills":1,"_id":0})
                idealSkills = idealSkillList['Skills']
                status = reqScorer(reqParsed,jobid,idealSkills,requisition)

        ##Working on Classified Job Id's
        jobId = list(db.category_candidate_map.find({"candidates":candidateId}).distinct("global_job_category_id"))
        for id in jobId:
            #print("Working on Job Category %s" %(id))
            #requisitionList = list(db.requisition.find({"new_global_job_category_id": id,"pre_identified_req": False},{"requisition_id":1,"_id":0}))
            requisitionList = list(db.requisition.find({"new_global_job_category_id": id,"pre_identified_req": False,"req_status_id" : {"$in":statusID}},{"requisition_id":1,"_id":0}))
            if(not requisitionList):
                #print("No Requisition for this Job ID - %s " % (id))
                continue
            idealSkillList = db.ideal_candidate_characteritics.find_one({"global_job_category_id":id},{"Skills":1,"_id":0})
            if (idealSkillList is None):
                idealSkills = 'NA'
            else:
                idealSkills = idealSkillList['Skills']
            for requisition in requisitionList:
                #print("Working on Requisition %s for Job Category %s" %(requisition["requisition_id"],id))
                reqParsed = list(db.requisition_skills_from_parsed_requisition.find({"requisition_id": requisition["requisition_id"]},{"parsedWords":1,"_id":0}))
                #print("Working on Requisition - %s" % requisition)
                if(len(reqParsed)==0):
                    #print("No Requisition Description - Time Elapsed %s" % (time.time() - reqTime))
                    continue
                status = candidateScorer(candIdList, reqParsed, idealSkills, requisition["requisition_id"])

        return("Scoring Complete")

    except Exception as e:
        DebugException(e)
        print("Candidate Incremental Scorer failed due to error [" + str(e) + "]")


