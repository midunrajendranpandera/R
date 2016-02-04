import json
import pymongo
from datetime import datetime, date, time
from pymongo import ReturnDocument
from pymongo import MongoClient
from debugException import DebugException
from zcSummaryObj import ZcSummaryObj

def writeScoresToDb(db, zScoreList, zSummary):

   try:
      ### First, save the cadidate score list
      for zScore in zScoreList:
         reqId = zScore["requisition_id"]
         candId = zScore["candidate_id"]
         db.candidate_zindex_score.replace_one({'requisition_id': reqId, 'candidate_id': candId }, zScore, True)

      ### Now, save the summary for the request   
      db.candidate_zindex_score_summary.replace_one( {"requisition_id" : reqId}, zSummary, True )
      summaryDoc = db.candidate_zindex_score_summary.find_one( {"requisition_id" : reqId}, { "_id" : 1} )
      summaryDocId = str((summaryDoc["_id"]))
      summaryObj = ZcSummaryObj(summaryDocId, True, "Search request completed successfully")
      return summaryObj.toJson()
   except Exception as e:
      DebugException(e)
      summaryObj = ZcSummaryObj(-1, False, e)
      print(summaryObj.toJson())
      return(summaryObj.toJson())
      raise


def writeScoresToAudit(db, zScoreList, zSummary, inputParamObj, startTime):

   try:
      ### First, save the cadidate score list
      inputObj = inputParamObj.toJson()
      zSummaryAudit = zSummary
      zSummaryAudit["start_time"] = startTime
      zSummaryAudit["end_time"] = datetime.now().isoformat()
      zSummaryAudit["input_parameters"] = inputObj
      db.candidate_zindex_score_audit.insert_many( zScoreList )
      db.candidate_zindex_score_summary_audit.insert_one( zSummaryAudit )
      summaryObj = ZcSummaryObj(0, True, "Write to Audit collection successful")
      return summaryObj.toJson()
   except Exception as e:
      DebugException(e)
      summaryObj = ZcSummaryObj(-1, False, e)
      return(summaryObj.toJson())
      raise
