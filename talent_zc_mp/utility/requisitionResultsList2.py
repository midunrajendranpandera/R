__author__ = 'Pandera'

#! /usr/local/bin/python3.4

import pymongo
from pymongo import MongoClient
from datetime import datetime

# DEV
# uri="mongodb://candidateuser:bdrH94b9tanQ@devmongo01.zcdev.local:27000/candidate_model"
# client=MongoClient(uri)
# db=client.candidate_model


# QA
uri = "mongodb://qacandidateuser:hpbVLH6j@qamongodb01.zcdev.local:27000,qamongodb02.zcdev.local:27000,qamongodb03.zcdev.local:27000/candidate_model_qajupiter"
client = MongoClient(uri)
db = client.candidate_model_qajupiter
beginTime = datetime.now()

print("[" + datetime.now().isoformat() + "] Report started ...")

#reqId = 188181
#reqId = 191674 
#reqId = 197989
#reqId = 198196
#reqId = 153473
#reqId = 167062
#reqId = 178027
#reqId = 100612
#reqId = 89177
#reqId = 195897
#reqId = 176121
#reqId = 100612
#reqId = 153473
#reqId = 167062
#reqId = 176121
#reqId = 178027
#reqId = 195897
#reqId = 89177


client_table = db["client"]
reqIdList = [100612, 153473, 167062, 176121, 178027, 195897, 89177]

for reqId in reqIdList:
   print("Requisition ID: [" + str(reqId) + "]")

   candList = list(db.candidate_zindex_score.find( { "requisition_id": reqId } ) )
   reqDetails = db.requisition.find_one({"requisition_id" : reqId }, {"new_global_job_category_id":1,"new_global_job_category_name":1,"_id":0})
   globalCatId = reqDetails["new_global_job_category_id"]
   globalCatName = reqDetails["new_global_job_category_name"]

   requisitionResultListFile = str(reqId)+"_RequisitionResultsList.log"
   resultLog = open(requisitionResultListFile, "w")

   vendorProjection = {"client_id": 1, "vendor_id": 1, "supplier_name": 1, "master_supplier_id": 1, "vendor_status_id": 1, "talent_cloud_agreement": 1, "supplier_type_id": 1, "_id": 0 }

   resultLog.write("RequisitionId|RequisitionGlobalCatId|RequisitionGlocalCatName|ClientId|ClientName|CandidateId|CandidateName|ReqZindexScore|Requisition_Submission_Status|Master_Supplier_ID|Supplier Name|SupplierAgreementID|SupplierAgreementStatus|SupplierAgreementClient|Supplier Type|ReqCandidate DNR|OptInTalentSearch|AllowTalentCloudSearchForAllDivision|ReqCandidateDivision" + "\n")
   #print("RequisitionId|ClientId|ClientName|CandidateId|CandidateName|Master_Supplier_ID|Supplier Name|SupplierAgreementID|SupplierAgreementStatus|SupplierAgreementClient|Supplier Type|Requisition Candidate DNR|ReqCandidate.OptInTalentSearch|ReqCanddate.AllowTalentCloudSearchForAllDivision|ReqCandidateDivision")
   for cand in candList:
      clientId = cand["client_id"]
      reqId = cand["requisition_id"]
      candId = cand["candidate_id"]
      fn = cand["candidate_first_name"]
      ln = cand["candidate_last_name"]
      candName = fn + " " + ln
      st = cand["supplier_type"]
      sn = cand["supplier_name"]
      zs = cand["zindex_score"]
      zd = cand["zindex_distribution"]
      clientId = cand["client_id"]
      client_name = client_table.find_one({"client_id": clientId}, {"client_name": 1, "_id": 0})
      client_name = client_name["client_name"]
      candDnr = db.candidate.count({"candidate_id": candId, "dnrs": clientId})
      msp = cand["master_supplier_id"]
      supplierName = cand["supplier_name"]
      suppType = cand["supplier_type"]
      candDetail = db.candidate.find_one({"candidate_id": candId}, {"opt_in_talent_search": 1, "allow_talent_cloud_search_for_all_division": 1, "_id": 0})
      optIn = candDetail["opt_in_talent_search"]
      allowTalent = candDetail["allow_talent_cloud_search_for_all_division"]
      req_sub_id = db.requisition_candidate.find_one({"requisition_id":reqId,"candidate_id":candId}, {"requisition_submission_id":1,"_id":0})
      if(req_sub_id is None):
         req_sub_id = {}
      if(len(req_sub_id)==0):
         req_sub_id = "-"
      else:
         req_sub_id = req_sub_id["requisition_submission_id"]
      if allowTalent:
         allowTalentDivision = 1
      else:
         allowTalentDivision = 0

      reqCandDiv = db.requisition_candidate_division.find_one({"candidate_id": candId, "client_id":clientId }, {"req_candidate_division_id": 1,"_id": 0})
      #vendor = db.vendor.find_one( { "client_id": clientId, "master_supplier_id": msp }, vendorProjection )
      vendor = db.vendor.find_one( { "req_client_id": clientId, "master_supplier_id": msp }, vendorProjection )

      if (vendor is None):
         suppAggId = 0
         suppAggStatus = 0
         suppAggClient = 0
      else:
         suppAggId = vendor["vendor_id"]
         suppAggStatus = vendor["vendor_status_id"]
         suppAggClient = vendor["client_id"]

      # print("%s|%s|%s|%s|%s|%s|%s|%s",(reqId,candId,fn,ln,st,sn,zs,zd))
      # print(candDnr)
      resultLog.write(str(reqId) + "|" + str(globalCatId) + "|" + str(globalCatName) + "|" + str(clientId) + "|" + str(client_name) +  "|" + str(candId) + "|" + str(candName) + "|" + str(zs) + "|" + str(req_sub_id) + "|" + str(msp) + "|" + str(supplierName)  + "|" + str(suppAggId) +  "|" + str(suppAggStatus) +  "|" + str(suppAggClient) + "|" + str(suppType) + "|" +  str(candDnr) + "|" + str(optIn) + "|" + str(allowTalentDivision) + "|" + str(reqCandDiv) + "\n")
      #print(str(reqId) + "|" + str(clientId) + "|" + str(client_name) +  "|" + str(candId) + "|" + str(candName) + "|" + str(msp) + "|" + str(supplierName) + "|" + str(suppAggId) +  "|" + str(suppAggStatus) +  "|" + str(suppAggClient) + "|" + str(suppType) + "|" +  str(candDnr) + "|" + str(optIn) + "|" + str(allowTalentDivision) + "|" + str(reqCandDiv))


print("[" + datetime.now().isoformat() + "] Report completed.")

print("Total time elapsed: [" + str(datetime.now() - beginTime) + "]  seconds.")

