import pyodbc
from datetime import datetime
from pymongo import MongoClient
import json
import bson

### QA
uri = "mongodb://qacandidateuser:hpbVLH6j@qamongodb01.zcdev.local:27000,qamongodb02.zcdev.local:27000,qamongodb03.zcdev.local:27000/candidate_model_qajupiter"
client=MongoClient(uri)
db=client.candidate_model_qajupiter

### DEV
#uri = "mongodb://candidateuser:bdrH94b9tanQ@devmongo01.zcdev.local:27000/?authSource=candidate_model"
#client=MongoClient(uri)
#db=client.candidate_model

candidate_list = list(db.candidate_skills_from_parsed_resumes.distinct("candidate_id"))

for candidate in candidate_list:
    candidateSkill = list(db.candidate.find({"candidate_id":candidate}, {"candidate_id":1,"job_skill_names": 1,"_id":0}))
    candResumeParsed = db.candidate_skills_from_parsed_resumes.find_one({"candidate_id":candidate})
    parsedSkills = candResumeParsed["parsedWords"]
    for skillList in candidateSkill:
        skillTemp = {}
        for skills in skillList["job_skill_names"]:
            skillTemp["count"] = 1
            skillTemp["word"] = skills["job_skill_name"]
            skillTemp["interpretet_value"] = 'skills'
            parsedSkills.append(skillTemp)
    candResumeParsed["parsedWords"] = parsedSkills
    key = {}
    key["candidate_id"] = candidate
    db.candidate_skills_from_parsed_resumes.update(key,candResumeParsed,upsert=True)
        
	