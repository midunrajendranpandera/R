import pymongo, json
from pymongo import MongoClient
import time
from datetime import datetime, date, time
from debugException import DebugException
from zcSummaryObj import ZcSummaryObj

### This module generates a summary response output when a search call is made.
### Assumption is that the process has searched the candidates, generated the Zindex scores
### and persisted the details in the collection candidate_zindex_score


### This function expects the zindexScoreList input
### as the list with zindexScores included in the collection
### and generates the summary based on the scores
def getSummary(reqId, zindexScoreList, searchAndScoreFlag):

   summaryDoc = {}

   try:

      zindexSum = 0
      maxScore = 0
      minScore = 100
      bestFit_90_100 = 0
      strongFit_80_89 = 0
      goodFit_70_79 = 0
      fairFit_60_69 = 0
      weakestFit_1_59 = 0
      candCnt = 0
      dataCenter = ""

      for zindexScore in zindexScoreList:
         dataCenter = zindexScore["data_center"]
         zScore = zindexScore["zindex_score"]
         #candidateId = zindexScore["candidate_id"]
         #print("CandidateId [" + str(candidateId) + "]  zindexScore [" + str(zScore) + "]")

         if searchAndScoreFlag:
            if zScore > 0:
               candCnt += 1
               #print("Candidate Count [" + str(candCnt) + "]")
         else:
            candCnt += 1

         zindexSum += zScore
   
         if (zScore > maxScore):
             maxScore = zScore
   
         if (zScore > 0 and zScore < minScore):
             minScore = zScore
         
         if (zScore >= 1 and zScore <= 59):
            weakestFit_1_59 += 1
            
         if (zScore >= 60 and zScore <= 69):
            fairFit_60_69 += 1

         if (zScore >= 70 and zScore <= 79):
            goodFit_70_79 += 1

         if (zScore >= 80 and zScore <= 89):
            strongFit_80_89 += 1

         if (zScore >= 90):
            bestFit_90_100 += 1

      if candCnt > 0:
         zindexAverage = int(zindexSum / candCnt)
      else:
         zindexAverage = 0
         minScore = 0
         maxScore = 0
       
      distList = []
      dist = {}
      dist["name"] = "Best Fit: 90 to 100"
      dist["value"] = bestFit_90_100
      distList.append(dist)
      dist = {}
      dist["name"] = "Strong Fit: 80 to 89"
      dist["value"] = strongFit_80_89
      distList.append(dist)
      dist = {}
      dist["name"] = "Good Fit: 70 to 79"
      dist["value"] = goodFit_70_79
      distList.append(dist)
      dist = {}
      dist["name"] = "Fair Fit: 60 to 69"
      dist["value"] = fairFit_60_69
      distList.append(dist)
      dist = {}
      dist["name"] = "Weakest Fit: 1 to 59"
      dist["value"] = weakestFit_1_59
      distList.append(dist)
   
      summaryDoc["requisition_id"] = reqId
      summaryDoc["data_center"] = dataCenter
      summaryDoc["created_date"] = datetime.now()
      summaryDoc["total_candidate"] = candCnt
      summaryDoc["highest_zindex_score"] = maxScore
      summaryDoc["lowest_zindex_score"] = minScore
      summaryDoc["average_zindex_score"] = zindexAverage
      summaryDoc["score_summary"] = distList
      summaryDoc["last_modified_date"] = datetime.now()

      #print("Scoring complete...")
      #print(str(summaryDoc))

   except Exception as e:
      message =  "Error while gathering summary! [" + str(e) + "]"
      DebugException(e)
      summaryObj = ZcSummaryObj(-1, False, message)
      
   return (summaryDoc)

def getZindexSummary(db, requisitionId, zindexScoreList, searchAndScoreFlag):

   start_time = time()
   beginTime = time()

   try:

      if (zindexScoreList):
         ### If the candidate score list is passed in
         summaryDoc = getSummary(requisitionId, zindexScoreList, searchAndScoreFlag)

      else:
         ### If the list is not passed, then fetch the list from the database collection
         zindexScoreList = list(db.candidate_zindex_score.find( {"requisition_id": requisitionId} ) )
         summaryDoc = getSummary(requisitionId, zindexScoreList, searchAndScoreFlag)
      
      endTime = time()
      #print("[DebugInfo] -- Process Start [" + beginTime.isoformat() + "]  End [" + endTime.isoformat() + "]   Elapsed [" + (time() - start_time) + "seconds]" )
      return(summaryDoc)

   except Exception as e:
      summaryObj = ZcSummaryObj(-1, False, e)
      DebugException(e)
      print("Error while gathering summary! [%s]" % e)
      return (summaryObj)

