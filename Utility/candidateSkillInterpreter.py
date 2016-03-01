import pyodbc
from datetime import datetime
from pymongo import MongoClient
import json
import bson

### QA
uri = "mongodb://qacandidateuser:hpbVLH6j@qamongodb01.zcdev.local:27000,qamongodb02.zcdev.local:27000,qamongodb03.zcdev.local:27000/candidate_model_qajupiter"
client=MongoClient(uri)
db=client.candidate_model_qajupiter

working_list = list(db.candidate_skills_from_parsed_resumes.find({"parsedWords.interpretet_value" : {"$exists":True}}))
for cand in working_list:
    key = {}
    out_data = {}
    print("Working on candidate %s" %cand["candidate_id"])
    key["candidate_id"] = cand["candidate_id"]
    out_data["candidate_id"] = cand["candidate_id"]
    key["resume_id"] = cand["resume_id"]
    out_data["resume_id"] = cand["resume_id"]
    out_data["datacenter"] = cand["datacenter"]
    parsedWords = cand["parsedWords"]
    for words in parsedWords:
        for key in words.keys():
            new_key = key.replace("interpretet_value","interpreter_value")
            if new_key != key:
                words[new_key] = words[key]
                del words[key]
    out_data["parsedWords"] = parsedWords
    db.candidate_skills_from_parsed_resumesupdate(key,out_data,upsert=True)

