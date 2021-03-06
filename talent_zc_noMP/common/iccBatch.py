__author__ = 'Pandera'
import sys
sys.path.append('../common')
import json
import time
import configparser
import operator
import math
from datetime import datetime
from pymongo import MongoClient
from dateutil import parser
from collections import Counter
from debugException import DebugException

config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db = client[db_name]

def insertIdealCharacteristics(idealskills, gjobcatid, reqidList, datacenter):
    for reqid in reqidList:
        idealtable = {}
        key = {}
        key["requisition_id"] = reqid
        idealtable["global_job_category_id"] = gjobcatid
        idealtable["Skills"] = idealskills
        idealtable["Region"] = datacenter
        idealtable["requisition_id"] = reqid
        db.ideal_candidate_characteritics.update(key,idealtable,upsert=True)


def ideal_characteristics_build(reqid):
    #Retreive Requisition Details
    reqDetails = list(db.requisition.find({"requisition_id": reqid},{"requisition_id": 1, "new_global_job_category_id": 1, "data_center":1, "_id": 0}))
    for requisition in reqDetails:
        gjobcatid = requisition["new_global_job_category_id"]
        datacenter = requisition["data_center"]
    # Find relevant requisitions
    relevantRequisitions = list(db.requisition.find({"new_global_job_category_id": gjobcatid},{"requisition_id": 1, "_id": 0}))
    reqidList = []
    for requisition in relevantRequisitions:
        reqidList.append(requisition["requisition_id"])
    # Find successful candidates
    # hiredCand = []
    hiredCandidates = list(db.requisition_candidate.find({"$or": relevantRequisitions, "is_hired": 1}, {"candidate_id": 1, "_id": 0}))
    if(len(hiredCandidates)==0):
        insertIdealCharacteristics("NA", gjobcatid, reqidList,datacenter)
        return("No Successful Candidates")
    # for candidate in hiredCandidates:
    #    hiredCand.append(candidate["candidate_id"])
    # Get all candidate resume words
    successfulResume = list(
        db.candidate_skills_from_parsed_resumes.find({"$or": hiredCandidates}, {"parsedWords": 1, "_id": 0}))
    cand_resume_skill_list = []
    for cand_resume_parsed in successfulResume:
        for cand_resume_skill in cand_resume_parsed["parsedWords"]:
            cand_resume_skill_list.append(cand_resume_skill["word"].lower())
    #resumeskillfreq = dict({word: cand_resume_skill_list.count(word) for word in cand_resume_skill_list})
    #sorted_resumeskills = sorted(resumeskillfreq.items(), key=operator.itemgetter(1), reverse=True)
    sorted_resumeskills = Counter(cand_resume_skill_list)
    skilllength = math.ceil((len(sorted_resumeskills) * 33) / 100)
    sorted_resumeskills = sorted_resumeskills.most_common(skilllength)
    idealskills = []
    for i in range(0, skilllength):
        skill = sorted_resumeskills[i]
        idealskills.append(skill[0])
    if(not idealskills):
        insertIdealCharacteristics("NA", gjobcatid, reqidList)
    else:
        insertIdealCharacteristics(idealskills, gjobcatid, reqidList)
    return("Success")

requisitionList = list(db.requisition.find({}).distinct("requisition_id"))
#requisitionList = [5]
reqcount = len(requisitionList)
counter = -1
for requisition in requisitionList:
    print ("Working on Requisition %s." % requisition)
    counter += 1
    print ("Working on Requisition# %s of %s" %(counter,reqcount))
    exist = db.ideal_candidate_characteritics_temp.count({"requisition_id":requisition})
    if(exist==1):
        print("Ideal Characteristics Exist")
        continue
    status = ideal_characteristics_build(requisition)
    print(status)
