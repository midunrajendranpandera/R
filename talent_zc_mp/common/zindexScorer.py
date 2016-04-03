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
from multiprocessing import Lock, Process, Queue, current_process,Manager,JoinableQueue
import multiprocessing
import itertools
import math

config = configparser.ConfigParser()
config.read('./common/ConfigFile.properties')

#CONSTANTS
HISTORY_WORDS_MATCH_NOISE = config.getfloat("ScoringParametersSection","HISTORY_WORDS_MATCH_NOISE")
HISTORY_SKILLS_MATCH_NOISE = config.getfloat("ScoringParametersSection","HISTORY_SKILLS_MATCH_NOISE")
HISTORY_IDEAL_MATCH_NOISE = config.getfloat("ScoringParametersSection","HISTORY_IDEAL_MATCH_NOISE")

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db = client[db_name]

startTime = time.time()

#def workerNoResume(candNoResume,requisition):

def workerNoResume(work_queue,requisition,done_queue):
    zindex_score_map_worker_no_resume = {}
    #candList = []
    #print("Inside No Resume Worker")
    #for candidate in iter(work_queue.get, 'STOP' ):
    #    candList.append(candidate)
    
    #print("Inside No Resume Worker %s" %(len(candList)))
    
    for candidate in iter(work_queue.get, 'STOP' ):
        key = {}
        zindex_score = {}
        zindex_skill_score = {}
        zindex_exp_score = {}
        zindex_jobfit_score = {}
        zindex_distribution = []
        ThreadTest = {}
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
        zindex_score_map_worker_no_resume[candidate] = zindex_score
    #print(zindex_score_map_worker_no_resume)
    if(zindex_score_map_worker_no_resume is not None):
        done_queue.update(zindex_score_map_worker_no_resume)
    #done_queue.put(zindex_score_map_worker_no_resume)
    #done_queue.close()

def workerWithResume(work_queue_resume, reqParsedSkillList,reqParsedWordsList,requisition,idealSkills,candidate_resume_parsed_mgr, done_queue_resume):
    zindex_score_map_resume = {}
    #print("Inside Resume Worker")
    #print("Inside Resume Worker %s" %(len(candidate_resume_parsed_mgr)))
    #print(candidate_resume_parsed_mgr)
    for candidate_resume in iter(work_queue_resume.get, 'STOP' ):
        cand_resume_parsed = candidate_resume_parsed_mgr[candidate_resume]
    #for cand_resume_parsed in  candidate_resume_parsed_mgr:
        key = {}
        zindex_score = {}
        zindex_skill_score = {}
        zindex_exp_score = {}
        zindex_jobfit_score = {}
        zindex_distribution = []
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

        ##cand_resume_parsed["parsedWords"]
        #parsedWords = candResumeParsed["candidate"]["parsedWords"]

        for cand_resume_skill in cand_resume_parsed["parsedWords"]:
            cand_resume_words_list.append(cand_resume_skill["word"].lower().strip())
            if(cand_resume_skill["interpreter_value"]=="skills"):
                cand_resume_skill_list.append(cand_resume_skill["word"].lower().strip())
        #candidateSkill = list(db.candidate.find({"candidate_id":cand_resume_parsed["candidate_id"]}, {"candidate_id":1,"job_skill_names": 1,"_id":0}))
        #for skillset in candidateSkill:
            #for skills in skillset["job_skill_names"]:
                #print(skills["job_skill_name"])
                #cand_resume_skill_list.append(skills["job_skill_name"])
        resWordsLength = HISTORY_WORDS_MATCH_NOISE * (len(reqParsedWordsList))
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
        if(zindex_jobfit_score["score"] > 20):
            zindex_jobfit_score["score"] = 20
        zindex_distribution = [zindex_skill_score,zindex_exp_score,zindex_jobfit_score]
        zindex_score["zindex_score"] = zindex_skill_score["score"]  + zindex_exp_score["score"] + zindex_jobfit_score["score"]
        zindex_score["zindex_distribution"] = zindex_distribution
        #zindex_score["update_datetime"] = datetime.utcnow()
        zindex_score_map_resume[zindex_score["candidate_id"]] = zindex_score
        #print("Requisition %s Candidate %s Inserting Scores" %(requisition,zindex_score["candidate_id"] ))
        #db.requisition_cand_zindex_scores.update(key,zindex_score,upsert=True)
    #done_queue_resume.put(zindex_score_map_resume)
    #done_queue_resume.close()
    if(zindex_score_map_resume is not None):
        done_queue_resume.update(zindex_score_map_resume)
    #return(zindex_score_map_resume)


def zeroInserter(requisition,candidate_id_list):
    #subCand = list(db.requisition_candidate.find({"requisition_id":requisition},{"candidate_id":1,"_id":0}))
    zindex_score_map = {}
    for candidate in candidate_id_list:
        #print(candidate)
        #print(type(candidate))
        zindex_score = {}
        key = {}
        zindex_distribution = []
        zindex_skill_score = {}
        zindex_exp_score = {}
        zindex_jobfit_score = {}
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
        zindex_score_map[candidate] = zindex_score
        #zindex_score["update_datetime"] = datetime.utcnow()
        #print("Requisition %s Candidate %s Inserting Zeroes [No Requisition]" %(requisition,candidate))
        #db.requisition_cand_zindex_scores.update(key,zindex_score,upsert=True)
    return(zindex_score_map)

def zindexScorer(reqParsed,candidate_id_list,db,idealSkills,requisition):

    candResumeParsed = list(db.candidate_skills_from_parsed_resumes.find({"candidate_id":{"$in": candidate_id_list}}, {"candidate_id": 1, "parsedWords": 1, "_id": 0}))
    #print("Fetched Resumes")
    candResumeParsedDict = {}
    for candidate in candResumeParsed:
        tempDict = {}
        tempDict[candidate["candidate_id"]] = candidate
        candResumeParsedDict.update(tempDict)
    #print("Converted to Dict")
    candResumeList = []
    for candidate in candResumeParsed:
        candResumeList.append(candidate["candidate_id"])

    zindex_score_map = {}
    candNoResume = []
    totalCandSubmission = set(candidate_id_list)
    candWithResume = set(candResumeList)
    candNoResume = list(totalCandSubmission-candWithResume)

    reqParsedSkillList = []
    reqParsedWordsList = []
    for req_parsed in reqParsed:
        for req_parsed_skill in req_parsed["parsedWords"]:
            reqParsedWordsList.append(req_parsed_skill["word"].lower().strip())
            if(req_parsed_skill["interpreter_value"]=="skills"):
                reqParsedSkillList.append(req_parsed_skill["word"].lower().strip())

    ###############################################
    ###### Call worker for No Resume################
    ###############################################

    ###############################################
    ## Call worker for candidates with Resume######
    ###############################################

    #workers = math.ceil(len(candidate_id_list) / 50)
    workers = 3
    mgr = Manager()
    work_queue = JoinableQueue()
    done_queue = mgr.dict()
    work_queue_resume = JoinableQueue()
    done_queue_resume = mgr.dict()
    processes = []
    processes_resume = []
    candidate_resume_parsed_mgr = mgr.dict()
    candidate_resume_parsed_mgr = candResumeParsedDict
    candResumeParsedDict = {}
    for candidate in candNoResume:
        work_queue.put(candidate)

    for candidate in candResumeList:
        work_queue_resume.put(candidate)

    for w in range(workers):
        p = Process(target=workerNoResume, args=(work_queue, requisition, done_queue))
        #p.daemon = True
        #p_resume = Process(target=workerWithResume, args=(work_queue_resume, reqParsedSkillList,reqParsedWordsList,requisition,idealSkills, candResumeParsedDict,done_queue_resume))
        processes.append(p)
        #processes_resume.append(p_resume)
        p.start()
        #p_resume.start()
        #processes.append(p)
        work_queue.put('STOP')
        #work_queue_resume.put('STOP')

    #work_queue.join()
    #done_queue.join()
    #done_queue.close()
    #work_queue.close()


    for p in processes:
        #print(p.is_alive())
        p.join()
        #print("Joinin No Resume Worker Process")

    #done_queue.put('STOP')    

    for w in range(workers):
        p_resume = Process(target=workerWithResume, args=(work_queue_resume, reqParsedSkillList,reqParsedWordsList,requisition,idealSkills, candidate_resume_parsed_mgr,done_queue_resume))
        processes_resume.append(p_resume)
        p_resume.start()
        processes.append(p)
        work_queue_resume.put('STOP')
    
    #work_queue_resume.join()
    #done_queue_resume.join()
    #done_queue_resume.close()
    #work_queue_resume.close()

    for p in processes_resume:
        #print(p.is_alive())
        p.join()

    #done_queue_resume.put('STOP')
    #print(done_queue_resume)

    ############################################################################
    ####Combine the dict objects from both the workers into zindex_score_map####
    ############################################################################

    #for scores in iter(done_queue_resume.get, 'STOP'):
    #    if( scores is None):
    #        continue
    #    zindex_score_map.update(scores)
    zindex_score_map.update(done_queue)
    zindex_score_map.update(done_queue_resume)
    return(zindex_score_map)

def scoreRetriever(reqId, candidate_id_list, score_collection_name):
    print("Score Entry Time %s" %(datetime.now()))
    jobid = list(db.requisition.find({"requisition_id":reqId},{"new_global_job_category_id":1,"_id":0}))
    reqParsed = list(db.requisition_skills_from_parsed_requisition.find({"requisition_id":reqId},{"parsedWords":1,"_id":0}))
    for words in reqParsed:
        if(words["parsedWords"] == []):
            reqParsed = []
    if(len(reqParsed) == 0):
        zindex_score_map = zeroInserter(reqId,candidate_id_list)
        return(zindex_score_map)        
    for id in jobid:
        gjid1 = id["new_global_job_category_id"]
    idealSkillList = db.ideal_candidate_characteritics.find_one({"global_job_category_id":gjid1},{"Skills":1,"_id":0})
    idealSkills = idealSkillList['Skills']
    print("Zindex Score Entry Time %s" %(datetime.now()))
    zindex_score_map = zindexScorer(reqParsed,candidate_id_list,db,idealSkills,reqId)
    print("Zindex Score Exit Time %s" %(datetime.now()))
    return(zindex_score_map)

