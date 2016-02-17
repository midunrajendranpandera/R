import sys
import configparser
from datetime import datetime
import writeToSqlServer
from pymongo import MongoClient

config = configparser.ConfigParser()
config.read('./ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db=client[db_name]

def mongoPreFilter(client_id):
    #strt_time = datetime.now()
    #clientIdList = [client_id]
    #candidate1 = list(db.candidate.find({ "opt_in_talent_search": 1 ,"dnrs":{"$not":{"$elemMatch":{"client_id":client_id}}}}).distinct("candidate_id"))
    candidate1 = list(db.candidate.find({ "opt_in_talent_search": 1 ,"dnr_client_ids": { "$ne":client_id}}).distinct("candidate_id"))
    candidate2 = list(db.candidate.find({ "allow_talent_cloud_search_for_all_division": True }).distinct("candidate_id"))
    cand1 = list(db.candidate.find({ "allow_talent_cloud_search_for_all_division": False }).distinct("candidate_id"))
    cand2 = list(db.requisition_candidate_division.find({"client_id": client_id, "active": True}).distinct("candidate_id"))
    cand3 = list(set(cand1).intersection(set(cand2)))
    candidate2 = list(set(candidate2 + cand3))
    candidate3 = list(db.candidate.find({"master_supplier_id": -1}).distinct("candidate_id"))
    query_dict = {
        "$or":[{"client_id": client_id, "talent_cloud_agreement": 0, "status_id": {"$in": [2,3]}},
               {"client_id": client_id, "talent_cloud_agreement": 1, "status_id": {"$in": [1,2,3]}},
               {"client_id": 30, "status_id": {"$in": [2,3]}}
            ]
    }
    #msp1 = list(db.vendor_master.find({"client_id": client_id, "talent_cloud_agreement": 0, "status_id": {"$in": [2,3]  } }).distinct("master_supplier_id"))
    #msp2 = list(db.vendor_master.find({"client_id": client_id, "talent_cloud_agreement": 1, "status_id": {"$in": [1,2,3] } }).distinct("master_supplier_id"))
    #msp3 = list(db.vendor_master.find({"client_id": 30, "status_id": {"$in": [2,3]} }).distinct("master_supplier_id"))
    #msp = list(set(msp1 + msp2 + msp3))
    msp = list(db.vendor_master.find(query_dict).distinct("master_supplier_id"))
    cand1 = list(db.candidate.find({ "master_supplier_id": { "$in": msp } }).distinct("candidate_id"))
    candidate3 = list(set(candidate3+cand1))
    preFilterCandidate = set(candidate1).intersection(set(candidate2))
    preFilterCandidate = list(set(preFilterCandidate).intersection(set(candidate3)))
    #print("Elapsed Time for Client[%s] is [%s]secs" %(client_id,(datetime.now()-strt_time)))
    return(preFilterCandidate)


client_table = db["client"]
client_id_list = list(client_table.distinct("client_id"))
#print(client_id_list)
#client_id_list = [314, 323, 324, 392, 513, 653, 761, 3260, 3282, 3328, 5000]
client_id_list = [356,345]
for client_id in client_id_list:
    key = {}
    key["client_id"] = client_id
    candidateDict = {}
    mongoStrtTime = datetime.now()
    mongoPreFilterCandList = mongoPreFilter(client_id)
    mongoTime = datetime.now() - mongoStrtTime
    candidateDict["mongo_prefilter"] = mongoPreFilterCandList
    sqlStrtTime = datetime.now()
    sqlPreFilterCandList = writeToSqlServer.getPreFilterCandidateList(client_id)
    sqlTime = datetime.now() - sqlStrtTime 
    candidateDict["sql_prefilter"] = sqlPreFilterCandList
    m_s_diff = list(set(mongoPreFilterCandList) - set(sqlPreFilterCandList))
    candidateDict["mongo-sql"] = m_s_diff
    s_m_diff = list(set(sqlPreFilterCandList) - set(mongoPreFilterCandList))
    common = list(set(sqlPreFilterCandList).intersection(set(mongoPreFilterCandList)))
    candidateDict["sql-mongo"] = s_m_diff
    candidateDict["client_id"] = client_id
    candidateDict["mongo_time"] = str(mongoTime)
    candidateDict["sql_time"] = str(sqlTime)
    candidateDict["intersection"] = common
    print(client_id)
    db.pre_filter_check_new.update(key,candidateDict,upsert=True)

