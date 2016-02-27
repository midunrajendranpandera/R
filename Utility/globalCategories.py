import pyodbc
from datetime import datetime
from pymongo import MongoClient
import json
import bson

### QA
#uri = "mongodb://qacandidateuser:hpbVLH6j@qamongodb01.zcdev.local:27000,qamongodb02.zcdev.local:27000,qamongodb03.zcdev.local:27000/candidate_model_qajupiter"
#client=MongoClient(uri)
#db=client.candidate_model_qajupiter

### DEV
uri = "mongodb://candidateuser:bdrH94b9tanQ@devmongo01.zcdev.local:27000/?authSource=candidate_model"
client=MongoClient(uri)
db=client.candidate_model

resultLog = open("JobCategorisation.csv", "w")
gjid = list(db.requisition_new.distinct("new_global_job_category_id"))
#print("Global Job Category Id | Number of Reqs \n")
resultLog.write("Global Job Category Id | Number of Reqs")
for id in gjid:
    req_list = list(db.requisition_new.find({"new_global_job_category_id":id}).distinct("requisition_id"))
    #print("Global Job Category Id [%s] - Number of relevant Req [%s] \n" %(id,len(req_list)) )
    #print("%s|%s\n"%(id,len(req_list)))
    resultLog.write("%s|%s\n"%(id,len(req_list)))
