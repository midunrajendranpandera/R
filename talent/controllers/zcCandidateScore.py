#! /usr/local/bin/python3.4
import sys
sys.path.append('./common')
import json
import pymongo
import bson
import configparser
from datetime import datetime, date, time
from pymongo import ReturnDocument
from bottle import route, run, request, response
from bson import Binary, Code, json_util
from bson.json_util import dumps
from bson.objectid import ObjectId
from pymongo import MongoClient
import candidateScorer
from zcSummaryObj import ZcSummaryObj
from zindexSummary import getZindexSummary
from debugException import DebugException
from dbWrite import writeScoresToDb
from dbWrite import writeScoresToAudit
from inputParams import InputParamsObj
from ZCLogger import ZCLogger
from writeToSqlServer import writeCandidateScoreToSqlServer

config = configparser.ConfigParser()
config.read('./common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db=client[db_name]
score_collection_name = "requisition_cand_zindex_scores"
logger = ZCLogger()

searchAndScoreFlag = False

def cleanScores(scored_candidates):
  for cand in scored_candidates:
    for key in cand.keys():
      #print("%s - %s - %s" % (key, type(cand[key]), sys.getsizeof(cand[key])))
      if type(cand[key]) is datetime:
        cand[key] = cand[key].isoformat()

  return scored_candidates

def getCandidateScore(reqId, isSubmitted, masterSupplierId, minScore):
   try:
      start_time = datetime.now()
      beginTime = datetime.now().isoformat()

      inputObj = InputParamsObj(reqId, isSubmitted, masterSupplierId, minScore)
      req = db.requisition.find_one( { "requisition_id" : int(reqId) } )
      if req is None:
         summaryObj = ZcSummaryObj(-1, False, "Invalid Requisition Id")
         return(summaryObj.toJson())


      ### Search for all submitted candidates
      if (int(reqId) > 0 and isSubmitted and int(masterSupplierId) <= 0):
          query_dict = {
                          "requisition_id" : int(reqId),
                          "dnr_flag" : False
                       }
          candidateList = db.requisition_candidate.find( query_dict, { "loaded_date" : 0, "updated_date" : 0, "dnr_flag" : 0, "is_hired" : 0, "_id" : 0 } )
          appendReq = False
          mspFlag = False
          
      ### Search submitted candidates with the provided master supplier id
      elif int(reqId) > 0 and isSubmitted and int(masterSupplierId) > 0:
          query_dict = {
              "requisition_id" : int(reqId),
              "dnr_flag" : False,
              "master_supplier_id" : int(masterSupplierId)
          }
          candidateList = db.requisition_candidate.find( query_dict, { "loaded_date" : 0, "updated_date" : 0, "dnr_flag" : 0, "is_hired" : 0, "_id" : 0 } )
          appendReq = False
          mspFlag = False

      #print(reqId, isSubmitted, masterSupplierId, minScore)

      ### Search all candidates with the provided master supplier id whether submitted or not.
      elif int(reqId) > 0 and (not isSubmitted) and int(masterSupplierId) > 0:
         print(reqId, isSubmitted, masterSupplierId, minScore)
         clientId = req["client_id"]
         exclude_list = {
                "_id": 0,
                "address1": 0,
                "address2": 0,
                "allow_talent_cloud_search_for_all_division": 0,
                "opt_in_talent_search": 0,
                "unique_resource_id": 0,
                "candidate_divisions": 0,
                "update_date" : 0,
                "dnrs": 0,
                "job_skill_names.job_skill_id" : 0,
                "job_skill_names.competency_years_experience" : 0,
                "job_skill_names.competency_level" : 0,
                "job_certificate_names.job_certification_id" : 0
         }
         candidateList = db.candidate.find( { "dnrs.client_id" : {"$ne" : clientId}, "master_supplier_id" : int(masterSupplierId) }, exclude_list )
         appendReq = True
         mspFlag = True

      else:
         summaryObj = ZcSummaryObj(0, True, "Invalid search parameters received")
         candidateList = None

      #logger.log(candidateList)
      cnt = 0 
      if (candidateList is not None):
         for c in candidateList:
            cnt += 1
      ### Now, if we have a condidate list, then process them with scoring
      if (cnt > 0):
         zindexScores = list(candidateScorer.scoreCandidates(req, candidateList, appendReq, score_collection_name, mspFlag, searchAndScoreFlag))
         #logger.log(str(zindexScores))
         ### Now, remove from the list who scored below minScore specified
         if minScore is not None and minScore is not "":
            zindexScoresMinScore = []
            for candidate in zindexScores:
               if candidate["zindex_score"] >=  int(minScore):
                  zindexScoresMinScore.append(candidate)
            zindexScores = zindexScoresMinScore

         ### Now, get the summary for requisition search
         summary = getZindexSummary(db, reqId, zindexScores, searchAndScoreFlag)
         
         ### Add input parameters to the results
         for zindex in zindexScores:
            zindex["input_is_submitted"] = int(isSubmitted) 
            zindex["input_master_supplier_id"] = int(masterSupplierId)
            zindex["input_min_score"] = int(minScore)

         summary["input_is_submitted"] = int(isSubmitted) 
         summary["input_master_supplier_id"] = int(masterSupplierId)
         summary["input_min_score"] = int(minScore)

         ### Finally write the results to the DB
         clean_zindex_scores = cleanScores(zindexScores)
         summaryDocId = writeCandidateScoreToSqlServer(int(reqId), int(isSubmitted), int(masterSupplierId), clean_zindex_scores, summary)
         if summaryDocId > 0:
            summaryObj = ZcSummaryObj(summaryDocId, True, "Search request completed successfully")
         else:
            summaryObj = ZcSummaryObj(summaryDocId, False, "Error in SQL Server write function")

         auditWriteResult = writeScoresToAudit(db, zindexScores, summary, inputObj, start_time)
      else:
         summaryObj = ZcSummaryObj(0, True, "No candidates found with the specified search criteria")
      
      print("%s" % summaryObj.toJson())

   except Exception as e:
      DebugException(e)
      summaryObj = ZcSummaryObj(-1, False, e)
      print("%s" % summaryObj.toJson())
      raise

   elapsed = datetime.now() - start_time
   endTime = datetime.now().isoformat()
   logger.log("[DebugInfo] -- [CandidateScore] Process Start [" + beginTime + "]  End [" + endTime + "]   Elapsed [" + str(elapsed) + "] seconds")

   return (summaryObj.toJson())
