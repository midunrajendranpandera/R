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
HISTORY_MATCH_NOISE = 0.65

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db = client[db_name]

startTime = time.time()

def scoreInserter(requisition):
    subCand = list(db.requisition_candidate.find({"requisition_id":requisition},{"candidate_id":1,"_id":0}))
    for candidate in subCand:
        #print(candidate)
        #print(type(candidate))
        zindex_score = {}
        key = {}
        zindex_distribution = []
        zindex_skill_score = {}
        zindex_exp_score = {}
        zindex_jobfit_score = {}
        zindex_distribution = []
        key["requisition_id"] = requisition
        key["candidate_id"] = candidate["candidate_id"]
        zindex_score["candidate_id"] =  candidate["candidate_id"]
        zindex_score["requisition_id"] = requisition
        zindex_skill_score["name"] = "Skills"
        zindex_skill_score["score"] = 0
        zindex_exp_score["name"] = "Experience"
        zindex_exp_score["score"] = 0
        zindex_jobfit_score["name"] = "Job Fit"
        zindex_jobfit_score["score"] = 0
        zindex_score["zindex_score"] = 0
        zindex_distribution = [zindex_skill_score,zindex_exp_score,zindex_jobfit_score]
        zindex_score["zindex_distribution"] = zindex_distribution
        print("Requisition %s Candidate %s Inserting Zeroes [No Requisition]" %(requisition,candidate["candidate_id"] ))
        db.requisition_cand_zindex_scores.update(key,zindex_score,upsert=True)

def reqScorer(reqParsed,jobid,idealSkills,requisition):
    reqParsedSkillList = []
    reqParsedWordsList = []
    for req_parsed in reqParsed:
        for req_parsed_skill in req_parsed["parsedWords"]:
            reqParsedWordsList.append(req_parsed_skill["word"].lower().strip())
            if(req_parsed_skill["interpreter_value"]=="skills"):
                reqParsedSkillList.append(req_parsed_skill["word"].lower().strip())

    subCand = list(db.requisition_candidate.find({"requisition_id":requisition},{"candidate_id":1,"_id":0}))
    subCandList = []
    for candidate in subCand:
        subCandList.append(candidate["candidate_id"])
    candResumeParsed = list(db.candidate_skills_from_parsed_resumes.find({"$or": subCand}, {"candidate_id": 1, "parsedWords": 1, "_id": 0}))
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

    # set operation example  a - b   # letters in a but not in b
    totalCandSubmission = set(subCandList)
    candWithResume = set(candResumeList)
    candNoResume = list(totalCandSubmission-candWithResume)
    for candidate in candNoResume:
        key["requisition_id"] = requisition
        key["candidate_id"] = candidate
        zindex_score["candidate_id"] =  candidate
        zindex_score["requisition_id"] = requisition
        zindex_skill_score["name"] = "Skills"
        zindex_skill_score["score"] = 0
        zindex_exp_score["name"] = "Experience"
        zindex_exp_score["score"] = 0
        zindex_jobfit_score["name"] = "Job Fit"
        zindex_jobfit_score["score"] = 0
        zindex_score["zindex_score"] = 0
        zindex_distribution = [zindex_skill_score,zindex_exp_score,zindex_jobfit_score]
        zindex_score["zindex_distribution"] = zindex_distribution
        print("Requisition %s Candidate %s Inserting Zeroes [No Resume]" %(requisition,candidate ))
        db.requisition_cand_zindex_scores.update(key,zindex_score,upsert=True)

    #Candidate resume skills
    cand_resume_skill_list = []
    #Candidate resume words
    cand_resume_words_list = []
    zindex_score = {}

    for cand_resume_parsed in candResumeParsed:
        zindex_score = {}
        #Candidate resume skills
        cand_resume_skill_list = []
        #Candidate resume words
        cand_resume_words_list = []
        zindex_score["candidate_id"] =  cand_resume_parsed["candidate_id"]
        zindex_score["requisition_id"] = requisition
        key = {}
        key["requisition_id"] = requisition
        key["candidate_id"] = cand_resume_parsed["candidate_id"]
        zindex_distribution = []
        zindex_skill_score = {}
        zindex_exp_score = {}
        zindex_jobfit_score = {}
        for cand_resume_skill in cand_resume_parsed["parsedWords"]:
            cand_resume_words_list.append(cand_resume_skill["word"].lower().strip())
            if(cand_resume_skill["interpreter_value"]=="skills"):
                cand_resume_skill_list.append(cand_resume_skill["word"].lower().strip())
        candidateSkill = list(db.candidate.find({"candidate_id":cand_resume_parsed["candidate_id"]}, {"candidate_id":1,"job_skill_names": 1,"_id":0}))
        for skillset in candidateSkill:
            for skills in skillset["job_skill_names"]:
                #print(skills["job_skill_name"])
                cand_resume_skill_list.append(skills["job_skill_name"])
        resWordsLength = HISTORY_MATCH_NOISE * (len(reqParsedWordsList))
        canWordsLength = len(cand_resume_words_list)
        wordsIntersection = len(set(reqParsedWordsList).intersection(set(cand_resume_words_list)))
        zindex_skill_score["name"] = "Skills"
        zindex_skill_score["score"] = math.ceil(40*(wordsIntersection/resWordsLength))
        if(zindex_skill_score["score"] > 40):
            zindex_skill_score["score"] = 40
        resSkillLength = len(reqParsedSkillList)
        skillIntersection = len(set(reqParsedSkillList).intersection(set(cand_resume_skill_list)))
        zindex_exp_score["name"] = "Experience"
        if(resSkillLength==0):
            zindex_exp_score["score"] = 0
        else:
            zindex_exp_score["score"] = math.ceil(40*(skillIntersection/resSkillLength))
        zindex_jobfit_score["name"] = "Job Fit"
        idealSkillLength = HISTORY_MATCH_NOISE * len(idealSkills)
        idealIntersection = len(set(idealSkills).intersection(set(cand_resume_words_list)))
        if(idealSkillLength == 0):
            zindex_jobfit_score["score"] = 0
        else:
            zindex_jobfit_score["score"] = math.ceil(20*(idealIntersection/idealSkillLength))
        if(zindex_jobfit_score["score"] > 20):
            zindex_jobfit_score["score"] = 20
        zindex_distribution = [zindex_skill_score,zindex_exp_score,zindex_jobfit_score]
        zindex_score["zindex_score"] = zindex_skill_score["score"]  + zindex_exp_score["score"] + zindex_jobfit_score["score"]
        zindex_score["zindex_distribution"] = zindex_distribution
        print("Requisition %s Candidate %s Inserting Scores" %(requisition,zindex_score["candidate_id"] ))
        db.requisition_cand_zindex_scores.update(key,zindex_score,upsert=True)

    return("Complete")

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
            resWordsLength = HISTORY_MATCH_NOISE * (len(reqParsedWordsList))
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
            resSkillLength = len(reqParsedSkillList)
            skillIntersection = len(set(reqParsedSkillList).intersection(set(cand_resume_skill_list)))
            zindex_exp_score["name"] = "Experience"
            if(resSkillLength==0):
                zindex_exp_score["score"] = 0
            else:
                zindex_exp_score["score"] = math.ceil(40*(skillIntersection/resSkillLength))
            zindex_jobfit_score["name"] = "Job Fit"
            idealSkillLength = HISTORY_MATCH_NOISE * len(idealSkills)
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
