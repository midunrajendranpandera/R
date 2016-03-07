import sys
sys.path.append('../common')
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
config.read('../common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")

text_file = open("category_classifier_QA3_75.json", "w")

client=MongoClient(uri)

db = client[db_name]

def obj_dict(self):
    return self.__dict__

def matchSkills(cand_skills, req_skills, threshold):
	cand_skills_length = len(cand_skills)
	cand_skill_list = []
	try:
		count = 0
		for skill in cand_skills:
			if(skill["job_skill_name"] in req_skills):
				count += 1

				if count >= (threshold * len(cand_skill_list)):
					return True
	except Exception as e:
		DebugException(e)

	return False

def getCharacteristicMap():
	ideal_table = db["ideal_candidate_characteritics"]
	characteristic_map = {};
	characteristic_id_list = list(ideal_table.find({}).distinct("global_job_category_id"))
	count = 0
	for id in characteristic_id_list:
		count += 1
		print("%s" % count)
		skill_list = ideal_table.find_one({"global_job_category_id": id}, {"Skills": 1})
		characteristic_map[id] = skill_list

	print("%s" % characteristic_map)

	return characteristic_map

def main():
	print("GO")
	start_time = time.time()
	document_map = {}
	try:
		cand_table = db["candidate"]
		#characteristic_id_list = list(ideal_table.find({}, {"global_job_category_id": 1, "Skills": 1}).distinct("global_job_category_id"))
		characteristic_list = getCharacteristicMap()
		
		#print("%s" % characteristic_list)
		print("%s" % len(characteristic_list))
		print("---query Time: %s seconds ---" % (time.time() - start_time))
		print("done")
		skip_amount = 0
		cand_count = 0
		total_cand = cand_table.count()
		print("%s" % total_cand)
		start_delta = 200000
		total_cand = 300000

		while (start_delta + skip_amount) < total_cand:
			candidate_list = list(cand_table.find({}, {"candidate_id":1,"job_skill_names": 1}).skip(start_delta + skip_amount).limit(2500))

			for candidate in candidate_list:
				cand_count += 1
				cand_cat_count = 0
				print("Running candidate - %s" % cand_count)
				classification_array = []
				for key, record in characteristic_list.items():
					if matchSkills(candidate["job_skill_names"], record["Skills"], .75):
						cand_cat_count += 1
						if document_map.get(key) is None:
							document_map[key] = []
						if candidate["candidate_id"] not in document_map[key]:
							document_map[key].append(candidate["candidate_id"])
				print("Added to %s categories" % cand_cat_count)

			#print("%s" % json.dumps(document_map, default=obj_dict))
			skip_amount += 2500

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