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

config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')
#CONSTANTS
HISTORY_WORDS_MATCH_NOISE = config.getfloat("ScoringParametersSection","HISTORY_WORDS_MATCH_NOISE")
HISTORY_SKILLS_MATCH_NOISE = config.getfloat("ScoringParametersSection","HISTORY_SKILLS_MATCH_NOISE")
HISTORY_IDEAL_MATCH_NOISE = config.getfloat("ScoringParametersSection","HISTORY_IDEAL_MATCH_NOISE")

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db = client[db_name]

startTime = time.time()

def candidateScorer(candidateId,reqParsed,idealSkills,reqId):

    reqParsedSkillList = []
    reqParsedWordsList = []
    for req_parsed in reqParsed:
        for req_parsed_skill in req_parsed["parsedWords"]:
            reqParsedWordsList.append(req_parsed_skill["word"].lower().strip())
            if(req_parsed_skill["interpreter_value"]=="skills"):
                reqParsedSkillList.append(req_parsed_skill["word"].lower().strip())
    #print("Getting Parsed Resumes")
    candList = candidateId
    candList = [candList[i:i+5000] for i in range(0, len(candList), 5000)]
    for candidatesList in candList:
        candidateId = candidatesList
        candResumeParsed = list(db.candidate_skills_from_parsed_resumes.find({"candidate_id": {"$in": candidateId}}, {"candidate_id": 1, "parsedWords": 1, "_id": 0}))
        candResumeList = []
        for candidate in candResumeParsed:
            candResumeList.append(candidate["candidate_id"])

        zindex_score = {}
        key = {}
        zindex_distribution = []
        zindex_skill_score = {}
        zindex_exp_score = {}
        zindex_jobfit_score = {}
        zindex_distribution = []

        totalCandSubmission = set(candidateId)
        candWithResume = set(candResumeList)
        candNoResume = list(totalCandSubmission-candWithResume)
        for candidate in candNoResume:
            key["requisition_id"] = reqId
            key["candidate_id"] = candidate
            zindex_score["candidate_id"] =  candidate
            zindex_score["requisition_id"] = reqId
            zindex_skill_score["name"] = "Skills"
            zindex_skill_score["score"] = 0
            zindex_exp_score["name"] = "Experience"
            zindex_exp_score["score"] = 0
            zindex_jobfit_score["name"] = "Job Fit"
            zindex_jobfit_score["score"] = 0
            zindex_score["zindex_score"] = 0
            zindex_distribution = [zindex_skill_score,zindex_exp_score,zindex_jobfit_score]
            zindex_score["zindex_distribution"] = zindex_distribution
            #print("Requisition %s Candidate %s Inserting Zeroes [No Resume]" %(reqId,candidate ))
            db.searchscore_cand_zindex_scores.update(key,zindex_score,upsert=True)
        #Candidate resume skills
        cand_resume_skill_list = []
        #Candidate resume words
        cand_resume_words_list = []
        zindex_score = {}
        for cand_resume_parsed in candResumeParsed:
            zindex_score = {}
            zindex_score["candidate_id"] =  cand_resume_parsed["candidate_id"]
            zindex_score["requisition_id"] = reqId
            key = {}
            key["requisition_id"] = reqId
            key["candidate_id"] = cand_resume_parsed["candidate_id"]
            zindex_distribution = []
            zindex_skill_score = {}
            zindex_exp_score = {}
            zindex_jobfit_score = {}
            #Candidate resume skills
            cand_resume_skill_list = []
            #Candidate resume words
            cand_resume_words_list = []
            for cand_resume_skill in cand_resume_parsed["parsedWords"]:
                cand_resume_words_list.append(cand_resume_skill["word"].lower().strip())
                if(cand_resume_skill["interpreter_value"]=="skills"):
                    cand_resume_skill_list.append(cand_resume_skill["word"].lower().strip())
            candidateSkill = list(db.candidate.find({"candidate_id":cand_resume_parsed["candidate_id"]}, {"candidate_id":1,"job_skill_names": 1,"_id":0}))
            for skillset in candidateSkill:
                for skills in skillset["job_skill_names"]:
                    #print(skills["job_skill_name"])
                    cand_resume_skill_list.append(skills["job_skill_name"])
            resWordsLength = HISTORY_WORDS_MATCH_NOISE * (len(reqParsedWordsList))
            if(resWordsLength == 0):
                zindex_skill_score["name"] = "Skills"
                zindex_skill_score["score"] = 0
                zindex_exp_score["name"] = "Experience"
                zindex_exp_score["score"] = 0
                zindex_jobfit_score["name"] = "Job Fit"
                zindex_jobfit_score["score"] = 0
                zindex_score["zindex_score"] = 0
                zindex_distribution = [zindex_skill_score,zindex_exp_score,zindex_jobfit_score]
                zindex_score["zindex_distribution"] = zindex_distribution
                #print("Requisition %s Candidate %s Inserting Zeroes" %(reqId,zindex_score["candidate_id"] ))
                db.searchscore_cand_zindex_scores.update(key,zindex_score,upsert=True)
                continue
            canWordsLength = len(cand_resume_words_list)
            wordsIntersection = len(set(reqParsedWordsList).intersection(set(cand_resume_words_list)))
            zindex_skill_score["name"] = "Skills"
            zindex_skill_score["score"] = math.ceil(40*(wordsIntersection/resWordsLength))
            if(zindex_skill_score["score"] > 40):
                zindex_skill_score["score"] = 40
            resSkillLength = HISTORY_SKILLS_MATCH_NOISE * len(reqParsedSkillList)
            skillIntersection = len(set(reqParsedSkillList).intersection(set(cand_resume_skill_list)))
            zindex_exp_score["name"] = "Experience"
            if(resSkillLength==0):
                zindex_exp_score["score"] = 0
            else:
                zindex_exp_score["score"] = math.ceil(40*(skillIntersection/resSkillLength))
            if(zindex_exp_score["score"] > 40):
                zindex_exp_score["score"] = 40
            zindex_jobfit_score["name"] = "Job Fit"
            idealSkillLength = HISTORY_IDEAL_MATCH_NOISE * len(idealSkills)
            idealIntersection = len(set(idealSkills).intersection(set(cand_resume_words_list)))
            if(idealSkillLength == 0):
                zindex_jobfit_score["score"] = 0
            else:
                zindex_jobfit_score["score"] = math.ceil(20*(idealIntersection/idealSkillLength))
            zindex_distribution = [zindex_skill_score,zindex_exp_score,zindex_jobfit_score]
            zindex_score["zindex_score"] = zindex_skill_score["score"]  + zindex_exp_score["score"] + zindex_jobfit_score["score"]
            zindex_score["zindex_distribution"] = zindex_distribution
            #print("Requisition %s Candidate %s Inserting Scores" %(reqId,zindex_score["candidate_id"] ))
            db.searchscore_cand_zindex_scores.update(key,zindex_score,upsert=True)

    return("Candidates Scored")

##Main##
jobId = list(db.category_candidate_map.find({}).distinct("global_job_category_id"))
#jobId = [1032,1033]
statusID = [2,3,4,6,9,10,11,12]
#clientID = [344,345,3307,513,514,734]
for id in jobId:
    candidates = list(db.category_candidate_map.find({"global_job_category_id":id},{"candidates":1,"_id":0}))
    for candidate in candidates:
        candidateId = candidate["candidates"]
    
    if(len(candidateId)==0):
        print("No Candidates Classified")
        continue
    candidateId = list(set(candidateId))
    requisitionList = []
    #candResumeParsed = list(db.candidate_skills_from_parsed_resumes.find({"candidate_id": {"$in": candidateId}}, {"candidate_id": 1, "parsedWords": 1, "_id": 0}))
    #Candidate resume skills
    #cand_resume_skill_list = []
    #Candidate resume words
    #cand_resume_words_list = []
    ## - To run scoring for  all uncomment next line and comment line after next comment
    #requisitionList = list(db.requisition.find({"new_global_job_category_id": id,"pre_identified_req": False},{"requisition_id":1,"_id":0}))

    requisitionList = list(db.requisition.find({"new_global_job_category_id": id,"pre_identified_req": False,"req_status_id" : {"$in":statusID}},{"requisition_id":1,"_id":0}))

    #requisitionList = list(db.requisition.find({"new_global_job_category_id": id,"pre_identified_req": False,},{"requisition_id":1,"_id":0}))

    ## - Change the client ID's for client who are running Zindex Scoring

    #requisitionList = list(db.requisition.find({"new_global_job_category_id": id,"client_id":{"$in":clientID},"pre_identified_req": False,"req_status_id" : {"$in":statusID}},{"requisition_id":1,"_id":0}))

    if(not requisitionList):
        print("No Requisition for this Job ID - %s " % (id))
        continue
    #sampleReq = requisitionList[0]
    #sampleReq = sampleReq["requisition_id"]
    #idealSkillList = list(db.ideal_candidate_characteritics.find({"requisition_id":sampleReq},{"Skills":1,"_id":0}))

    idealSkillList = db.ideal_candidate_characteritics.find_one({"global_job_category_id":id},{"Skills":1,"_id":0})
    
    idealSkills = []
    idealSkills = idealSkillList['Skills']
    #for Skills in idealSkillList:
        #idealSkills.append(Skills["Skills"])
    #idealSkills = idealSkills[0]
    print("The number of Candidates %s for Job Id %s" %((len(candidateId),id)))
    for requisition in requisitionList:
        reqTime = time.time()
        reqParsed = list(db.requisition_skills_from_parsed_requisition.find({"requisition_id": requisition["requisition_id"]},{"parsedWords":1,"_id":0}))
        print("Working on Requisition - %s" % requisition["requisition_id"])
        #print("Parsed Resume %s" % reqParsed)
        for parsedwords in reqParsed:
            parsed = parsedwords['parsedWords']
        if(len(parsed)==0):
            #status = zeroInserter(requisition["requisition_id"],candidateId)
            print("No Requisition Description - Time Elapsed %s" % (time.time() - reqTime))
            continue
        #print("The number of Candidates %s for Job Id %s" %((len(candidateId),id)))
        status = candidateScorer(candidateId,reqParsed,idealSkills,requisition["requisition_id"])
print("---Total Time: %s seconds ---" % (time.time() - start_time))
