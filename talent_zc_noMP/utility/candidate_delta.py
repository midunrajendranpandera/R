import pyodbc
from datetime import datetime
from pymongo import MongoClient
import json
import bson


### QA
uri = "mongodb://qacandidateuser:hpbVLH6j@qamongodb01.zcdev.local:27000,qamongodb02.zcdev.local:27000,qamongodb03.zcdev.local:27000/candidate_model_qapluto"
client=MongoClient(uri)
db=client.candidate_model_qapluto

### DEV
#uri = "mongodb://candidateuser:bdrH94b9tanQ@devmongo01.zcdev.local:27000/?authSource=candidate_model"
#client=MongoClient(uri)
#db=client.candidate_model

conn = pyodbc.connect("DRIVER={FreeTDS};SERVER=QAPlutoSQL01.zcdev.local;PORT=1433;TDS_Version=7.2;UID=qatalent;PWD=z2Z*@&k(1(;DATABASE=One")

beginTime = datetime.now()
currenttime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
text_file = open("candidate_delta.txt", "w")
cursor = conn.cursor()

SQLCommand = ("SELECT DISTINCT [ReqCandID] FROM [One].[dbo].[ReqCandidate]")

cursor.execute(SQLCommand)
recs = cursor.fetchall()
print ("Recs Found : %s ", len(recs))
sqlCandidateList = []
for rec in recs:
   sqlCandidateList.append(rec[0])

conn.commit() 
conn.close()

mongoCandidateList = db.candidate.find({}).distinct("candidate_id")
diffCandidate = list(set(sqlCandidateList)-set(mongoCandidateList))

for candidate in diffCandidate:
   text_file.write("%s\n" %candidate)
endTime = datetime.now()

#print("BeginTime: [" + str(beginTime) + "] EndTime: [" + str(endTime) + "] ElapsedTIme: [" + str(endTime - beginTime) + "]")
print("Begin Time - %s : End Time - %s : CandidateDelta - %s : Elapsed Time - %s" %(beginTime,endTime,len(diffCandidate),(str(endTime - beginTime))))
