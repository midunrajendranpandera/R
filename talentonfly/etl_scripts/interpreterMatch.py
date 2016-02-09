import sys
sys.path.append('./common')
import re
import string
import pymongo
import json
import time
import configparser
from resumeResult import ResumeResult
from parsedWords import ParsedWord

start_time = time.time()

from pymongo import MongoClient

config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')

uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")

client=MongoClient(uri)

db = client[db_name]
reqs = db[]

print("%s" % db)

finalUpload = []
matchCount = 0
count = 0
cursor_start = 0
QUERY_LIMIT = 10
WRITE_TO_DB = False

text_file = open("output_local.json", "w")
text_file.write("[")
interpreterList = list(db.master_interpreter.find())
total_resumes = db.candidate_resume_text.count()
total_resumes = 3500
print(str(total_resumes))
word_count_list = []
wrote_first_record = True

def obj_dict(self):
    return self.__dict__

while cursor_start < total_resumes:
    resumeList = list(db.candidate_resume_text.find().skip(cursor_start).limit(3500))
    for resumeLine in resumeList:
        wordcount = {}
        count += 1
        print("Running "+ str(count))
        resumeText = resumeLine["resume_text"]
        #print("Text - %s" % resumeText)
        if resumeText is not None and resumeText is not "":
            candidateId = resumeLine["candidate_id"]
            resumeId = resumeLine["resume_id"]
            dataCenter = resumeLine["date_center"]
            currentResumeResult = ResumeResult(resumeId, candidateId, dataCenter)
            for interpreterLine in interpreterList:
                interpreterItem = " "+interpreterLine["Item"]+" "
                interpreterValue = interpreterLine["ItemType"]
                if interpreterItem is not None and interpreterItem is not "":
                    matchCount = resumeText.lower().count(interpreterItem.lower())
                    if matchCount > 0:
                        if interpreterValue != None:
                            currentResumeResult.parsedWords.append(ParsedWord(interpreterItem.lower().strip(), matchCount, interpreterValue.lower()))
                        else:
                            currentResumeResult.parsedWords.append(ParsedWord(interpreterItem.lower().strip(), matchCount, 0))

                        #resumeText = re.sub(interpreterItem.lower(),"",resumeText).lower().strip()

            for word in resumeText.lower().split():
                clean_word = word.strip()
                if clean_word not in wordcount:
                    wordcount[clean_word] = 1
                else:
                    wordcount[clean_word] += 1

            for word,match_count in wordcount.items():
                currentResumeResult.parsedWords.append(ParsedWord(word, match_count, 0))
             
            #word_count_list.append(currentResumeResult)

            print("---Parse Time: %s seconds ---" % (time.time() - start_time))
            resumeJSON = json.dumps(currentResumeResult, default=obj_dict)
            if wrote_first_record:
                text_file.write(",")
            else:
                wrote_first_record = True

            text_file.write("%s" % resumeJSON)
            print("count: %s - total: %s" % (count, total_resumes))

            print("---JSON Time: %s seconds ---" % (time.time() - start_time))
            mongoResume = json.loads(resumeJSON)
            print("---BSON Time: %s seconds ---" % (time.time() - start_time))

            if WRITE_TO_DB:
                reqs.insert_one(mongoResume)
            #word_count_list = []

    cursor_start += 3500
text_file.write("]")
text_file.close()
print("---Total Time: %s seconds ---" % (time.time() - start_time))