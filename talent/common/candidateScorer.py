import configparser
from random import randint
from debugException import DebugException
from pymongo import MongoClient
from ZCLogger import ZCLogger
from math import sin, cos, sqrt, atan2, radians
#from rzindex_wrapper import rzindex_wrapper

config = configparser.ConfigParser()
config.read('./common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")

client=MongoClient(uri)

db = client[db_name]

logger = ZCLogger()

zDist = {
            "zindex_distribution" : [
               {
                  "name" : "Experience",
                  "score" : 0
               },
               {
                  "name" : "Skills",
                  "score" : 0
               },
               {
                  "name" : "Job Fit",
                  "score" : 0
               }
            ],
            "zindex_score" : 0
         }

def renameZindex(zdist):
   #print(str(zdist))
   zd1={}
   zd2={}
   zd3={}
   zd=[]
   zdOld = zdist[0]
   zd1['name']=zdOld['Name']
   zd1['score']=zdOld['Score']
   zdOld = zdist[1]
   zd2['name']=zdOld['Name']
   zd2['score']=zdOld['Score']
   zdOld = zdist[2]
   zd3['name']=zdOld['Name']
   zd3['score']=zdOld['Score']
   zd=[zd1,zd2,zd3]
   #print(str(zd))
   return(zd)


def lookupCandidateScores(req_id, candidate_id_list, score_collection_name):
   score_map = {}
   score_table = db[score_collection_name]

   query_dict = {
      "requisition_id": req_id,
      "candidate_id":{
         "$in": candidate_id_list
      }
   }

   score_list = score_table.find(query_dict)
   for score in score_list:
      score_map[score["candidate_id"]] = score

   return score_map

def calcDistance(req_lat, req_lng, cnd_lat, cnd_lng):
   earth_radius = 3960

   lat1 = radians(req_lat)
   lng1 = radians(req_lng)
   lat2 = radians(cnd_lat)
   lng2 = radians(cnd_lng)

   dlon = lng2 - lng1
   dlat = lat2 - lat1

   a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
   c = 2 * atan2(sqrt(a), sqrt(1 - a))

   return int(earth_radius * c)



def getVendor(clientId, masterSupplierId):
   vendor = {}
   try:
      vendorProjection = {"client_id" : 1, "vendor_id" : 1, "supplier_name" : 1, "master_supplier_id" : 1, \
                          "vendor_status_id" : 1, "talent_cloud_agreement": 1, "supplier_type_id": 1, "_id" : 0 }
      vendor = db.vendor.find_one( { "client_id" : clientId, "master_supplier_id" : masterSupplierId }, vendorProjection )
      #print(str(vendor))

      if vendor is not None:
         if vendor["talent_cloud_agreement"] == 0:
            vendor["supplier_type"] = 1
         else:
            vendor["supplier_type"] = 2

         if vendor["master_supplier_id"] == -1:
            vendor["supplier_name"] = "Direct"
   except Exception as e:
      DebugException(e)

   return vendor

def getReqCandidateDivision(clientId, candidateId):
   reqCandidateDivision = {}
   reqCandidateDivision = db.requisition_candidate_division.find_one( { "client_id" : int(clientId), "candidate_id" : int(candidateId), "active" : True } )

   return reqCandidateDivision

def getSupplierType(clientId, masterSupplierId, candidateId):
   supplierType = {}
   reqCandDiv = {}

   try:
      supplierProjection = {"client_id" : 1, "vendor_id" : 1, "supplier_name" : 1, "master_supplier_id" : 1, \
                            "vendor_status_id" : 1, "talent_cloud_agreement": 1, "supplier_type_id": 1, "_id" : 0 }
      vendor_coll = db["vendor"]
      supplierType = vendor_coll.find_one( { "req_client_id" : int(clientId), "master_supplier_id" : int(masterSupplierId) }, supplierProjection )
      #print(str(supplierType))
      reqCandDiv = getReqCandidateDivision(int(clientId), int(candidateId))
      #print(str(reqCandDiv))

      if supplierType is not None:
         if (supplierType["vendor_id"] == -1 and reqCandDiv is None):
            supplierType["supplier_type_id"] = 2

         if supplierType["vendor_id"] == -1:
            supplierType["supplier_name"] = "Direct"
      #else:
      #   supplierType["supplier_type_id"] = 0
      #   supplierType["supplier_name"] = None

   except Exception as e:
      DebugException(e)

   return supplierType

def scoreCandidates(requisition, candidate_list, appendRequisition, score_collection_name, mspFlag, searchAndScoreFlag):
   return_list = []
   candidate_id_list = []
   score_map = {}
   reqId = requisition["requisition_id"]
   #print(reqId)

   client_table=db["client"]
   msp_company = db.client_table.find_one( { "client_id" : requisition["client_id"] }, { "msp_company_name": 1, "_id": 0 } )
   if msp_company is not None:
      msp_company_name = msp_company["msp_comnany_name"]
   else:
      msp_company_name = None

   candidate_list.rewind()
   for candidate in candidate_list:
      candidate_id_list.append(candidate["candidate_id"])

   rc_table = db["requistion_candidate"]
   submission_query_dict = {
      "requisition_id" : int(reqId), 
      "candidate_id" : {"$in": candidate_id_list }
   }

   #print("%s" % submission_dict)

   submission_map = {}
   #print(str(candidate_id_list))

   submission_list = list(db.requisition_candidate.find( submission_query_dict ) )
   #print("Submission list %s" % submission_list)

   submission_count_list = list(db.requisition_candidate.aggregate([
      {"$match":{"$and":[{"candidate_id" : {"$in": candidate_id_list}},
                {"requisition_submission_status_id":{"$in": [2, 4, 5, 6, 12, 13, 19]}}]}},
      {"$group":{
         "_id": "$candidate_id",
         "count":{"$sum": 1}
      }}
   ]))
   
   for sub in submission_list:
      submission_map[sub["candidate_id"]] = sub
      #print("Submitted Candidate [" + str(sub["candidate_id"]) + "] submitted_bill_rate ["+ str(sub["submitted_bill_rate"]) + "]")

   #print(str(submission_map))
   #print(str(submission_count_list))
   submission_count_map = {}
   for sub in submission_count_list:
      #print("Submitted Candidate [" + str(sub["_id"]) + "] Count ["+ str(sub["count"]) + "]")
      submission_count_map[sub["_id"]] = sub["count"]
      #print("Submitted Candidate [" + str(sub["_id"]) + "] Previous Submit count ["+ str(submission_count_map[sub["_id"]]) + "]")

   #print(str(submission_count_map))
   #print(reqId)

   try:
      score_map = lookupCandidateScores(reqId, candidate_id_list, score_collection_name)
      ### If the request is for master_supplier_id, then look for any submitted scores as well
      if mspFlag or searchAndScoreFlag:
         #print("mspFlag is TRUE")
         submitted_score_collection_name = "requisition_cand_zindex_scores"
         score_map2 = lookupCandidateScores(reqId, candidate_id_list, submitted_score_collection_name)
         #print (str(score_map2))
         if len(score_map2) > 0 or score_map2 is not None:
            for cand in candidate_id_list:
               try:
                  #print(str(score_map2[cand]))
                  sm = score_map2[cand]
                  if sm is not None:
                     #print("candidateId : [" + str(sm["candidate_id"]) + "] ZindexScore : [" + str(sm["zindex_score"]) + "]  ZindexDistribution : [" + str(sm["zindex_distribution"]) + "]")
                     #print(str(sm))
                     score_map[sm["candidate_id"]] = sm
               except Exception as e:
                  None

   except Exception as e:
      DebugException(e)
   
   candidate_list.rewind()
   for candidate in candidate_list:
      try:
         candidate_id = candidate["candidate_id"]

         try:
            candidate["zindex_score"] = score_map[candidate["candidate_id"]]["zindex_score"]
            candidate["zindex_distribution"] = score_map[candidate["candidate_id"]]["zindex_distribution"]
         except Exception as e:
            ### If no score found, then add default score
            candidate["zindex_score"] = zDist["zindex_score"]
            candidate["zindex_distribution"] = zDist["zindex_distribution"]

         if (appendRequisition):
            candidate["requisition_id"] = requisition["requisition_id"]
            candidate["client_id"] = requisition["client_id"]
            candidate["division"] = requisition["division"]
            candidate["work_country_code"] = requisition["req_country_code"]
            candidate["work_country_name"] = requisition["req_country_name"]
            candidate["work_postal_code"] = requisition["req_postal_code"]
            candidate["requisition_submit_date"] = requisition["req_submitted_date"]
            candidate["interview_status"] = 0
            candidate["zerochaos_bill_rate"] = 0.0
            candidate["units"] = 0
            candidate["offer_accepted"] = 0

            #VendorId, SupplierType and SupplierName details
            #vend = getVendor(requisition["client_id"], candidate["master_supplier_id"])
            supplier = getSupplierType(requisition["client_id"], candidate["master_supplier_id"], candidate["candidate_id"])
            if supplier is not None:
               candidate["vendor_id"] = supplier["vendor_id"]
               candidate["supplier_type"] = supplier["supplier_type_id"]
               candidate["supplier_name"] = supplier["supplier_name"]
            else:
               candidate["vendor_id"] = 0
               candidate["supplier_type"] = 0
               candidate["supplier_name"] = None
              
            for certificate in candidate["job_certificate_names"]:
               #print("%s" % certificate)
               certificate["is_required_cert"] = 0
               for req_cert in requisition["job_certificate_names"]:
                  if ( (certificate["job_certification_name"] == req_cert["job_certification_name"]) and (req_cert["is_required_cert"] == 1) ):
                     certificate["is_required_cert"] = 1

            for cand_skill in candidate["job_skill_names"]:
               #print("%s" % cand_skill)
               cand_skill["is_required_skill"] = 0
               for req_skill in requisition["job_skill_names"]:
                  if ( (cand_skill["job_skill_name"] == req_skill["job_skill_name"]) and ( req_skill["is_required_skill"] == 1) ):
                     cand_skill["is_required_skill"] = 1     

 
            if candidate["latitude"] is not None and requisition["req_latitude"] is not None:
               candidate["distance"] = calcDistance(float(requisition["req_latitude"]), float(requisition["req_longitude"]), float(candidate["latitude"]), float(candidate["longitude"]))
            else:
               candidate["distance"] = 0

            candidate["bill_currency"] = requisition["bill_currency"]

            #TODO - search submission list for candidate and use info here
            submission = {}
            if(submission_map):
               try:
                  submission = submission_map[candidate_id]
               except Exception as e:
                  submission = None
            else:
               submission = None

            if (submission is not None):
               candidate["requisition_submission_status_id"] = submission["requisition_submission_status_id"]
               candidate["requisition_submission_status"] = submission["requisition_submission_status"]
               candidate["candidate_submit_date"] = submission["candidate_submit_date"]
               candidate["resource_pay_rate"] = submission["resource_pay_rate"]
               #candidate["previous_submits"] = submission["count"]
               #candidate["previous_submits"] = submission_count_map["candidate_id"]
               candidate["submitted_bill_rate"] = submission["submitted_bill_rate"]
            else:
               candidate["requisition_submission_status_id"] = 0
               candidate["requisition_submission_status"] = None
               candidate["candidate_submit_date"] = None
               candidate["resource_pay_rate"] = 0.0
               #candidate["previous_submits"] = 0
               candidate["submitted_bill_rate"] = 0.0

            if (submission_count_map):
               try:
                  candidate["previous_submits"] = submission_count_map[candidate_id]
               except Exception as e:
                  candidate["previous_submits"] = 0

            #Added based on missing attribute Document
            candidate["client_ot_rate"] = 0
            candidate["supplier_ot_rate"] = 0
            candidate["work_city"] = requisition["req_city"]
            candidate["work_state_code"] = requisition["req_state_code"]
            candidate["work_state_name"] = requisition["req_state_name"]
            candidate["work_state_code_id"] = requisition["req_state_code_id"]
            candidate["msp_company_name"] = msp_company_name

      except Exception as e:
         DebugException(e)

      return_list.append(candidate)

   return return_list

