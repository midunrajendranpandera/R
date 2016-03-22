__author__ = 'Pandera'
import sys
sys.path.append('../../common')
import re
import string
import pymongo
import json
import time
import configparser
from classifierObject import ClassifierObject
from debugException import DebugException
from pymongo import MongoClient

config = configparser.ConfigParser()
config.read('../../common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")
client=MongoClient(uri)
db = client[db_name]

text_file = open("category_classifier_3.json", "w")
Threshold = 0.40

#client=MongoClient(uri)
#db = client[db_name]

def obj_dict(self):
    return self.__dict__

def matchSkills(cand_skills, req_skills, threshold):
    req_skills_length = len(req_skills)
    cand_skills_length = len(cand_skills)
    # print("matchSkills : "+ str(type(req_skills)))
    req_skills_set = set(req_skills)
    cand_skill_set = set(cand_skills)
    min_matching_criteria = 0.0
    try:
        count = 0
        candidate_match_count = len(list(cand_skill_set.intersection(req_skills_set)))
              # Amount of requisition skills is greater than the amount
        if req_skills_length > cand_skills_length:
            matching_criteria = req_skills_length * threshold
            # the macthing criteria still too big for the candidates skills
            # this could cause the candidates skill still would not reach the min criteria.
            # in this is the case we need to calibrate to the cand skills
            if matching_criteria > cand_skills_length:
                min_matching_criteria = cand_skills_length * (1 - threshold)
            else:
                min_matching_criteria = matching_criteria

        if req_skills_length < cand_skills_length:
            matching_criteria = cand_skills_length * threshold
            # the macthing criteria still too big for the req skills
            # this could cause the req skill still would not reach the min criteria.
            # in this is the case we need to calibrate to the req skills
            if matching_criteria > req_skills_length:
                min_matching_criteria = req_skills_length * (1 - threshold)
            else:
                min_matching_criteria = matching_criteria

        if req_skills_length == cand_skills_length:
            # the lenght are the same, either value will be good for calculation
            min_matching_criteria = req_skills_length * threshold
            # print("matchSkills : candidate_match_count [" + str(candidate_match_count) + "]")

        if candidate_match_count >= min_matching_criteria: #(req_skills_length * threshold):
            #print("MinMatchSkills : [" + str(min_matching_criteria) + "] matched [" + str(candidate_match_count) +
            #       "] reqSkillsCount [" + str(req_skills_length) + "] candSkillCount [" + str(cand_skills_length) +"]")
            return True

    except Exception as e:
        DebugException(e)

    return False

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
            #print("---Run Time: %s seconds ---" % (time.time() - start_time))

    return characteristic_map

def main():
    print("GO")
    start_time = time.time()
    document_map = {}
    try:
        cand_table = db["candidate_skills_from_parsed_resumes"]
        #characteristic_id_list = list(ideal_table.find({}, {"global_job_category_id": 1, "Skills": 1}).distinct("global_job_category_id"))
        characteristic_list = getCharacteristicMap()
        #print("%s" % characteristic_list)
        print("Total Number of Job Categories - %s" % len(characteristic_list))
        print("---Job Categories Retrieval Time: %s seconds ---" % (time.time() - start_time))
        #print("done")
        skip_amount = 0
        cand_count = 0
        total_cand = cand_table.count()
        print("%s" % total_cand)
        start_delta = 270000
        category_map = db["category_candidate_map"]
        #total_cand = 160000

        while (start_delta + skip_amount) < total_cand:
            candidate_list = list(cand_table.find({}, {"candidate_id":1,"parsedWords": 1}).skip(start_delta + skip_amount).limit(5000))
            for candidate in candidate_list:
                cand_count += 1
                cand_cat_count = 0
                parsedWords = candidate["parsedWords"]
                candidate_skill_list = []
                for words in parsedWords:
                    candidate_skill_list.append(words["word"].lower())
                printPerThousand = cand_count%1000
                if(printPerThousand == 0):
                    print("Running candidate - %s - Time Taken So Far - %s" % (cand_count,(time.time() - start_time)))
                    printPerThousand = 1
                #print("Running candidate - %s - Time Taken So Far - %s" % (cand_count,(time.time() - start_time)))
                #print("Running candidate - %s - Time Taken So Far - %s" % (cand_count,(time.time() - start_time)))
                classification_array = []
                for key, record in characteristic_list.items():
                    if matchSkills(candidate_skill_list, record["Skills"], Threshold):
                        cand_cat_count += 1
                        if document_map.get(key) is None:
                            document_map[key] = []
                        if candidate["candidate_id"] not in document_map[key]:
                            document_map[key].append(candidate["candidate_id"])
                            #category_map.update({"global_job_category_id":key},{"$addToSet":{"candidates":candidate["candidate_id"]}},True)
                #print("Added to %s categories" % cand_cat_count)

            #print("%s" % json.dumps(document_map, default=obj_dict))
            skip_amount += 5000

        count = 0
        text_file.write("[")
        for key, val in document_map.items():
            record = ClassifierObject(key, val)
            recordJson = record.toJson()
            text_file.write("%s" % recordJson)
            count += 1
            if count < len(document_map):
                text_file.write(",")

        text_file.write("]")
        text_file.close()

        print("---Parse Time: %s seconds ---" % (time.time() - start_time))

    except Exception as e:
        DebugException(e)
        print("---Run Time: %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__": main()
