#Python module to check connectivity with Mongo DB and SQL Server and return the status

##############################################################
# Service Return status:                                     #
#   1 - Service is OK                                        #
#  -1 - Service has issues with SQL Server connectivity      #
#  -2 - Service has issues with Mongo DB connectivity        #
#  -9 - Service has exception with SQL Server or Mongo       #
# -99 - Service has issues with both SQL Server and Mongo    #
##############################################################

import pyodbc
import time
from datetime import datetime
from debugException import DebugException
import configparser
from pymongo import MongoClient
from ZCLogger import ZCLogger

config = configparser.ConfigParser()
config.read('./common/ConfigFile.properties')
#config.read('./ConfigFile.properties')

driver=config.get("SQLDatabaseSection", "sql.driver")
server=config.get("SQLDatabaseSection", "sql.server")
port=config.get("SQLDatabaseSection", "sql.port")
tds_version=config.get("SQLDatabaseSection", "sql.tds_version")
uid=config.get("SQLDatabaseSection", "sql.uid")
pwd=config.get("SQLDatabaseSection", "sql.pwd")
database=config.get("SQLDatabaseSection", "sql.database")

connString = "DRIVER="+driver+";"+"SERVER="+server+";"+"PORT="+port+";"+"TDS_Version="+tds_version+";"+"UID="+uid+";"+"PWD="+pwd+";"+"DATABASE="+database
conn = pyodbc.connect(connString)
uri = config.get("DatabaseSection", "database.connection_string")
db_name = config.get("DatabaseSection", "database.dbname")

client=MongoClient(uri)
mongoDb = client[db_name]

logger = ZCLogger()


def getServiceStatus():

   sqlStatus = -9
   mongoStatus = -9
   serviceStatus = -9

   beginTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
   start_time = datetime.now()

   ### Check SQL Server connectivity
   try:
      cursor = conn.cursor()

      ### Check if there are any existing records with the same RequestID
      SQLCmd = ("SELECT count(*) as nv_count from  zcwNameValues ")
      cursor.execute(SQLCmd)
      recs = cursor.fetchall()
      nvCount = 0
      for rec in recs:
         nvCount = rec[0]
         ##print ("zcNameValues Count   : ", rec[0])
      logger.log("[getServiceStatus]  Count from [zcwNameValues] - [" + str(nvCount) + "]")
      if nvCount >= 0:
         sqlStatus = 1
      else:
         sqlStatus = -1

      conn.commit()
      #conn.close()

   except Exception as e:
      DebugException(e)
      sqlStatus = -9


   ### Check Mongo DB connectivity
   try:
      reqCount = -9
      reqCount = mongoDb.requisition.find().count()
      logger.log("[getServiceStatus]  Count from [requisition] - [" + str(reqCount) + "]")
      if reqCount >= 0:
         mongoStatus = 1
      else:
         mongoStatus = -2

   except Exception as e:
      DebugException(e)
      mongoStatus = -9

   if (sqlStatus == 1 and mongoStatus == 1):
      serviceStatus = 1
   else:
      if (sqlStatus != 1 and mongoStatus != 1):
         serviceStatus = -99
      elif (sqlStatus != 1):
         serviceStatus = sqlStatus
      elif (mongoStatus != 1):
         serviceStatus = mongoStatus

         
   #serviceStatus = 0

   endTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
   elapsedTime = datetime.now() - start_time
   logger.log("[getServiceStatus]   BeginTime  [" + beginTime + "]   EndTime [" + endTime + "]  Elapsed Time [" + str(elapsedTime) + "]  Service Status [" + str(serviceStatus) + "]")


   return (serviceStatus)
