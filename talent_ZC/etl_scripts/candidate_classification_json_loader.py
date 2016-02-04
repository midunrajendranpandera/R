import sys
sys.path.append('../common')
import re
import string
import pymongo
import json
import time
import configparser
from pprint import pprint
from classifierObject import ClassifierObject
from debugException import DebugException
from pymongo import MongoClient

config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")

text_file = open("category_classifier_QA4.json", "w")

client=MongoClient(uri)

db = client[db_name]

def obj_dict(self):
    return self.__dict__

def matchSkills(cand_skills, req_skills, threshold):
	try:
		count = 0
		for skill in cand_skills:
			if skill["job_skill_name"] in req_skills:
				count += 1
				if count == threshold:
					return True
	except Exception as e:
		DebugException(e)

	return False

def getCharacteristicMap():
	ideal_table = db["ideal_candidate_characteritics"]
	characteristic_map = {};
	print("here")
	characteristic_id_list = list(ideal_table.find({}).distinct("global_job_category_id"))
	print("waiting")
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
	category_map = db["category_candidate_map"]
	start_time = time.time()
	try:
		with open("category_classifier_QA4.json") as data_file:
			data = json.load(data_file)
			for d in data:
				print("Updating %s with %s candidates" % (d["global_job_category_id"], len(d["candidates"])))
				category_map.update( {"global_job_category_id" : d["global_job_category_id"]}, { "$push" : { "candidates": { "$each" : d["candidates"] } } } )

		print("---Parse Time: %s seconds ---" % (time.time() - start_time))

	except Exception as e:
		DebugException(e)
		print("---Run Time: %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__": main()