__author__ = 'Pandera'
import sys
sys.path.append('../common')
import json
import time
import configparser
import collections
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

#New Resume Text
newCandResume = db['_candidate_resume_text']
#candidateList= list(newCandResume.find({ "$and": [ { "resume_text": { "$nin": [""] } }, { "resume_text": { "$nin": [None] } } ] }).distinct("candidate_id"))
#candidateList= list(newCandResume.find({}).distinct("candidate_id"))
#candidateList= list(db.candidate_resume_text.find({ "$and": [ { "resume_text": { "$nin": [""] } }, { "resume_text": { "$nin": [None] } } ] }).distinct("candidate_id"))
#candidateList= list(db.candidate_resume_text.find({}).distinct("candidate_id"))
canParsedList = list(db.candidate_skills_from_parsed_resumes.find({}).distinct("candidate_id"))

canDiff = list(set(candidateList) - set(canParsedList))

candidateFile = open("candidateList.txt", "w")
for candidate in canDiff:
    candidateFile.write(str(candidate)+"\n")

