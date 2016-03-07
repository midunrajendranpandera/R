import sys

sys.path.append('../common')
import re
import string
import pymongo
import json
import time
from datetime import datetime
import configparser
from classifierObject import ClassifierObject
from debugException import DebugException
from pymongo import MongoClient
from rzindex_wrapper import rzindex_candidate



config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")

client = MongoClient(uri)

db = client[db_name]
fetch_limit = 100
JOB_NAME = "candidate_classifier"
beginTime = datetime.now()
jobs = list(db.etl_job_log.find({"job_name": JOB_NAME}).sort("start_datetime", -1).limit(1))
for j in jobs:
    job = j

lastRunDate = job["start_datetime"]
# log_file = open("candidateClassifierJob.log", "a")
etl_job_log = {}


def obj_dict(self):
    return self.__dict__


def matchSkills(cand_skills, req_skills, threshold):
    req_skills_length = len(req_skills)
    # print("matchSkills : "+ str(type(req_skills)))
    req_skills_set = set(req_skills)
    cand_skill_set = set(cand_skills)
    try:
        count = 0
        candidate_match_count = len(list(cand_skill_set.intersection(req_skills_set)))
        # print("matchSkills : candidate_match_count [" + str(candidate_match_count) + "]")
        if candidate_match_count >= (threshold * req_skills_length):
            # cand_skill_matched.append(req_skill)
            return True

    except Exception as e:
        DebugException(e)

    return False


# Get Ideal Characteristics Data to match againts the candidates
def getCharacteristicMap():
    ideal_table = db["ideal_candidate_characteritics"]
    characteristic_map = {}
    ideal_characteristics_list = list(
        ideal_table.find({"Skills.0": {"$exists": True}}).distinct("global_job_category_id"))
    # ideal_table.find({}).distinct("global_job_category_id"))
    count = 0

    for global_job_category_id in ideal_characteristics_list:
        try:
            count += 1
            # print("getCharacteristicMap : Record Count [" + str(count) + "] global_job_category_id[" +
            # str(global_job_category_id) + "]")
            skill_list = ideal_table.find_one({"global_job_category_id": global_job_category_id},
                                              {"Skills": 1, "_id": 0})

            characteristic_map[global_job_category_id] = skill_list
            # print ("getCharacteristicMap : jcId: " + str(global_job_category_id) + " <len:> %d"+
            # len(skill_list["Skills"]) + " Skills List:" + str(skill_list["Skills"]))
        except Exception as e:
            DebugException(e)
            print("---Run Time: %s seconds ---" % (time.time() - start_time))

    return characteristic_map


def main():
    print("GO")
    start_time = time.time()
    document_map = {}
    category_map = db["category_candidate_map"]
    try:
        cand_table = db["candidate"]
        characteristic_list = getCharacteristicMap()

        print("[candidateClassifierJob] ---query Time: %s seconds ---" % (time.time() - start_time))
        skip_amount = 0
        cand_count = 0
        total_cand = cand_table.find(
            {"$or": [{"loaded_date": {"$gt": lastRunDate}}, {"update_date": {"$gt": lastRunDate}}]}).count()
        # print("[candidateClassifierJob] [TotalCandidates]  %s" % total_cand)
        start_delta = 0

        query_dict = {
            "$and": [
                {"job_skill_names.0": {"$exists": True}},
                {"$or": [
                    {"loaded_date": {"$gt": lastRunDate}},
                    {"update_date": {"$gt": lastRunDate}}
                ]},
            ]
        }

        proj_dict = {"candidate_id": 1, "job_skill_names": 1}

        # candidate_list = list(
        # cand_table.find(query_dict, proj_dict).skip(start_delta + skip_amount).limit(fetch_limit))
        candidate_list = cand_table.find(query_dict, proj_dict)  # .distinct("candidate_id")

        candlist = []
        for candidate in candidate_list:
            candlist.append(candidate["candidate_id"])
            match_count = 0
            if len(candidate["job_skill_names"]) > 0:
                cand_count += 1
                cand_job_skill_name_list = []
                for can_job_skill in candidate["job_skill_names"]:
                    cand_job_skill_name_list.append(can_job_skill["job_skill_name"].lower())
                for job_cat_id, skill in characteristic_list.items():
                    # print("main : Running candidate [" + str(candidate["candidate_id"]) + "] reqSkillCount[" +
                    # str(len(skill["Skills"])) + "] vs candSkillsCount [" + str(len(cand_job_skill_name_list)) +"]")
                    if matchSkills(cand_job_skill_name_list, skill["Skills"], .40):
                        # print("main :        <<<MACTHED>> IdealCharac Section JobCatId[{0}] candidate_id [{1}".format(
                        # str(job_cat_id), str(candidate["candidate_id"])))
                        match_count += 1
                        if document_map.get(job_cat_id) is None:
                            document_map[job_cat_id] = []
                        if candidate["candidate_id"] not in document_map[job_cat_id]:
                            document_map[job_cat_id].append(candidate["candidate_id"])
                            # print("main : job_cat_id [" + str(job_cat_id) + "]  candidate_id [" + str(
                            # candidate["candidate_id"]) + "]")

        # print("Candidate_id [" + str(candidate["candidate_id"]) + " matched catids [" + str(match_count) + "]")
        # print("%s" % json.dumps(document_map, default=obj_dict))

        # Execute the update
        for cat_id, val in document_map.items():
            category_map.update({"global_job_category_id": cat_id}, {"$push": {"candidates": {"$each": val}}})
        
        print(candlist)        
        for cand in candlist:
            print(cand)
            rzindex_candidate(cand)


        # print("---Parse Time: %s seconds ---" % (time.time() - start_time))
        etl_job_log["job_name"] = JOB_NAME
        etl_job_log["start_datetime"] = beginTime
        etl_job_log["end_datetime"] = datetime.now()
        etl_job_log["elapsed_time_in_seconds"] = time.time() - start_time
        etl_job_log["total_records_processed"] = cand_count
        db.etl_job_log.insert_one(etl_job_log)
        # log_file.write(str(etl_job_log))
        timeNow = datetime.utcnow().isoformat()
        print("[" + timeNow + "] [candidateClassifierJob]  ---Run Time: [" + str(
            time.time() - start_time) + "] seconds ---")

    except Exception as e:
        DebugException(e)
        print("[candidateClassifierJob]  ---Run Time: %s seconds ---" % (time.time() - start_time))
        print("Job : [candidate_classifier] failed due to error [" + str(e) + "]")
        # log_file.write("---Run Time: %s seconds ---" % (time.time() - start_time))  # log_file.close()


if __name__ == "__main__": main()

