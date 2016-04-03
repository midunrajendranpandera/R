import pyodbc
from datetime import datetime
from pymongo import MongoClient
import json
import bson


### QA
uri = "mongodb://qacandidateuser:hpbVLH6j@qamongodb01.zcdev.local:27000,qamongodb02.zcdev.local:27000,qamongodb03.zcdev.local:27000/candidate_model_qapluto"
client=MongoClient(uri)
db=client.candidate_model_qapluto

conn = pyodbc.connect("DRIVER={FreeTDS};SERVER=QAPlutoSQL01.zcdev.local;PORT=1433;TDS_Version=7.2;UID=qatalent;PWD=z2Z*@&k(1(;DATABASE=One")

beginTime = datetime.now()
currenttime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
text_file = open("requisition_delta.txt", "w")
cursor = conn.cursor()

SQLCommand = ("SELECT DISTINCT [ReqID] FROM [One].[dbo].[Requisition]")

cursor.execute(SQLCommand)
recs = cursor.fetchall()
print ("Recs Found : %s ", len(recs))
sqlRequisitionList = []
for rec in recs:
   sqlRequisitionList.append(rec[0])

conn.commit() 
conn.close()

mongoRequisitionList = db.requisition.find({}).distinct("requisition_id")
diffRequisition = list(set(sqlRequisitionList)-set(mongoRequisitionList))

for requisition in diffRequisition:
   text_file.write("%s\n" %requisition)

endTime = datetime.now()

print("Begin Time - %s : End Time - %s : Requisition Delta - %s : Elapsed Time - %s" %(beginTime,endTime,len(diffRequisition),(str(endTime - beginTime))))
