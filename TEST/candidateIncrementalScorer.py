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
from

config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')
HISTORY_MATCH_NOISE = 0.7

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db = client[db_name]

def candidateIncrementalScorer(candidateId):
    try:
        candResumeParsed = list(db.candidate_skills_from_parsed_resumes.find({"candidate_id": candidateId}, {"candidate_id": 1, "parsedWords": 1, "_id": 0}))
        candResumeList = []
        for candidate in candResumeParsed:
            candResumeList.append(candidate["candidate_id"])
        zindex_score = {}
        key = {}
        zindex_distribution = []
        zindex_skill_score = {}
        zindex_exp_score = {}
        zindex_jobfit_score = {}
        zindex_distribution = []
        totalCandSubmission = set(candidateId)
        candWithResume = set(candResumeList)


