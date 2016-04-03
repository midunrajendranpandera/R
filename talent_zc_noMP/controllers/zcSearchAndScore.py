#! /usr/local/bin/python3.4
import sys
sys.path.append('./common')
import json
import bottle
import pymongo
import bson
import candidateScorer
import configparser
from datetime import datetime, date, time
from bottle import route, run, request, response
from bson import Binary, Code, json_util
from bson.json_util import dumps
from bson.objectid import ObjectId
from zcSummaryObj import ZcSummaryObj
from queryParams import QueryParams
from pymongo import MongoClient
from debugException import DebugException
from zindexSummary import getZindexSummary
from ZCLogger import ZCLogger
from inputParams import InputParamsObj
from dbWrite import writeScoresToDb
from dbWrite import writeScoresToAudit
#from writeToSqlServer import getPreFilterCandidateList

config = configparser.ConfigParser()
config.read('./common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db=client[db_name]
score_collection_name = "searchscore_cand_zindex_scores"
logger = ZCLogger()

mspFlag = False
searchAndScoreFlag = True

def getRequisitionObject(reqId):
	table = db["requisition"]
	return table.find_one({"requisition_id" : int(reqId)})

def getRelevantVendorIds(paramsObj):
	vendor_id_array = []
	vendor_table = db["vendor"]
	vendor_dict = {
		"$or":[
			{"client_id":paramsObj.client_id},
			{"client_id": 30}
		],
		"$or":[
			{"vendor_status_id" : 2},
			{"vendor_status_id" : 3}
		]
	}
	vendor_obj_list = list(vendor_table.find(vendor_dict, {"master_supplier_id" : 1}))
	for vendor in vendor_obj_list:
		vendor_id_array.append(vendor["master_supplier_id"])

	return vendor_id_array

def getCandidateList(paramsObj, relevant_vendor_ids, category_name, relevant_candidate_ids):
	candidate_table = db["candidate"]

	query_dict = {
		"$and":[
			{"candidate_id":
				{"$in": relevant_candidate_ids}
			},
			{"dnrs":
				{"$not":
					{"$elemMatch":
						{"client_id":paramsObj.client_id}
					}
				}
			},
			{"$or":[
					{"$and":[
							{"allow_talent_cloud_search_for_all_division":paramsObj.search_all_division},
							{"master_supplier_id": paramsObj.master_supplier_id}
						]
					},
					{"$and":[
							{"allow_talent_cloud_search_for_all_division":paramsObj.search_all_division},
							{"master_supplier_id": {"$in": relevant_vendor_ids}}
						]
					},
					{"$and":[
							{"candidate_divisions":{"$elemMatch":{"client_id":paramsObj.client_id}}},
							{"master_supplier_id": paramsObj.master_supplier_id}
						]
					},
					{"$and":[
							{"candidate_divisions":{"$elemMatch":{"client_id":paramsObj.client_id}}},
							{"master_supplier_id": {"$in": relevant_vendor_ids}}
						]
					}
				]
			},
			{"available": True}
		]		
	}

	predicate_dict = {
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
                "job_certificate_names.job_certification_id" : 0,
                "etl_process_flag" : 0
	}
	ex_time = datetime.now()
	candidate_cursor = db.candidate.find(query_dict,  predicate_dict )
	#print("---Cursor Time: %s seconds ---" % (datetime.now() - ex_time))
	return candidate_cursor

def mongoPreFilter(client_id):
    query_dict = {
            "$or":[{"client_id": client_id, "talent_cloud_agreement": False, "status_id": {"$in": [2,3]}},
                   {"client_id": client_id, "talent_cloud_agreement": True, "status_id": {"$in": [1,2,3]}},
                   {"client_id": 30, "status_id": {"$in": [2,3]}}
                ]
    }
    msp = list(db.vendor_master.find(query_dict).distinct("master_supplier_id"))
    query_dict = {
           "$or":[{ "master_supplier_id": { "$in": msp }},
                   {"master_supplier_id": -1}
                ]
    }
    candidate3 = list(db.candidate.find(query_dict).distinct("candidate_id"))
    query_dict = {
            "candidate_id":{"$in":candidate3},
            "$or":[{"allow_talent_cloud_search_for_all_division": True},
                   {"allow_talent_cloud_search_for_all_division": False,"candidate_divisions.client_id": client_id, "candidate_divisions.active": True},
                ]
    }
    candidate2 = list(db.candidate.find(query_dict).distinct("candidate_id"))
    #preFilterCandidate = list(set(candidate2).intersection(set(candidate3)))
    finalCandidateList = list(db.candidate.find({ "candidate_id":{"$in":candidate2},"opt_in_talent_search": 1 ,"dnr_client_ids": { "$ne":client_id}}).distinct("candidate_id"))
    #finalCandidateList = list(db.candidate.find({ "candidate_id":{"$in":preFilterCandidate},"opt_in_talent_search": 1 ,"dnr_client_ids": { "$ne":client_id}}).distinct("candidate_id"))
    return(finalCandidateList)

	
def getCandidateList2(final_candidate_ids):
        candidate_table = db["candidate"]

        query_dict = { "candidate_id": {"$in": final_candidate_ids} }

        predicate_dict = {
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
                "job_certificate_names.job_certification_id" : 0,
                "etl_process_flag" : 0,
                "dnr_client_ids" : 0
        }
        ex_time = datetime.now()
        candidate_cursor = db.candidate.find(query_dict,  predicate_dict )
        #print("---Cursor Time: %s seconds ---" % (datetime.now() - ex_time))
        return candidate_cursor


def getSubmittedCandidates(reqId):
	submittedCandList = []
	submittedCandListDict = list(db.requisition_candidate.find( { "requisition_id" : int(reqId) } , { "candidate_id" : 1, "_id" : 0 } ))	
	for subCand in submittedCandListDict:
		submittedCandList.append(subCand["candidate_id"])

	if submittedCandListDict is None:
		submittedCandList = []
		return submittedCandList

	return submittedCandList

def getRelevantCandidates(category_id):
	category_map_table = db["category_candidate_map"]
	relevant_cands = category_map_table.find_one({"global_job_category_id": category_id}, {"candidates" : 1})
	if relevant_cands is None:
		candidates = []
		return candidates
	return relevant_cands["candidates"]

def getSearchAndScore(reqId, minScore):
	start_time = datetime.now()
	beginTime = datetime.now().isoformat()

	inputObj = InputParamsObj(int(reqId), None, None, int(minScore))

	scored_candidate_list = []
	relevant_vendor_ids = []
	try:
		reqObj = getRequisitionObject(int(reqId))
		if reqObj is None:
			summaryObj = ZcSummaryObj(-1, False, "Invalid Requisition Id")
			return(summaryObj.toJson())

		client_id = reqObj["client_id"]
		category_id = reqObj["new_global_job_category_id"]
		relevant_candidate_ids = getRelevantCandidates(category_id)

		#if relevant_candidate_ids is None or len(relevant_candidate_ids) == 0:
			#summaryObj = ZcSummaryObj(0, True, "No candidates found under relevant job classification")
			#return(summaryObj.toJson())	

		#queryParams = QueryParams(client_id)
		#relevant_vendor_ids = getRelevantVendorIds(queryParams)
		#candidate_list = getCandidateList(queryParams, relevant_vendor_ids, category_id, relevant_candidate_ids)

		### Get prefiltered candidates from SQL Server
		#preFilterCandidateList = getPreFilterCandidateList(client_id)
		preFilterCandidateList = mongoPreFilter(client_id)

		### Get submitted candidates list from requisition_candidate collection
		submittedCandidateList = getSubmittedCandidates(reqId)

		### Convert the candidate lists into sets
		relevantCandidateSet = set(relevant_candidate_ids)
		preFilterCandidateSet = set(preFilterCandidateList)

		### get the intersection list of the two lists
		filteredCandidateList = list(preFilterCandidateSet.intersection(relevantCandidateSet) )

		### Finally, add the submitted candidate list to the filtered list
		finalCandidateList = filteredCandidateList + submittedCandidateList

		### Get the candidates info from candidate collection using the finalCandidateList
		candidate_list = getCandidateList2(finalCandidateList)

		if candidate_list is None:
			summaryObj = ZcSummaryObj(0, True, "No candidates found with the specified search criteria")
			return(summaryObj.toJson())	

		scored_candidate_list = candidateScorer.scoreCandidates(reqObj, candidate_list, True, score_collection_name, mspFlag, searchAndScoreFlag)

		if scored_candidate_list is not None and minScore is not None and minScore is not "":
			scored_list_minScore = []
			for candidate in scored_candidate_list:
				if candidate["zindex_score"] >= int(minScore):
					scored_list_minScore.append(candidate)
			scored_candidate_list = scored_list_minScore

		if scored_candidate_list is not None:
			summary = getZindexSummary(db, int(reqId), scored_candidate_list, searchAndScoreFlag)
			db.candidate_zindex_score.delete_many({"requisition_id": int(reqId)})
			db.candidate_zindex_score.insert_many(scored_candidate_list)

			db.candidate_zindex_score_summary.replace_one( {"requisition_id" : int(reqId)}, summary, True )
			summaryDoc = db.candidate_zindex_score_summary.find_one( {"requisition_id" : int(reqId)}, { "_id" : 1} )
			summaryDocId = str((summaryDoc["_id"]))
			summaryObj = ZcSummaryObj(summaryDocId, True, "Search request completed successfully")
			auditWriteResult = writeScoresToAudit(db, scored_candidate_list, summary, inputObj, start_time)
		else:
			summaryObj = ZcSummaryObj(0, True, "No candidates found with the specified search criteria")
			return(summaryObj.toJson())	

	except Exception as e:
		DebugException(e)
		summaryObj = ZcSummaryObj(-1, False, e)
		raise

	elapsed = datetime.now() - start_time
	endTime = datetime.now().isoformat()
	print("---Total Time: %s seconds ---" % (datetime.now() - start_time))
	logger.log("[DebugInfo] -- [SearchAndScore] RequisitionId [" + str(reqId) + "]   Process Start [" + beginTime + "]  End [" + endTime + "]   Elapsed [" + str(elapsed) + "] seconds")
	return (summaryObj.toJson())
