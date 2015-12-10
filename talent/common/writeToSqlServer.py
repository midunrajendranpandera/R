#Python module to connect to SQL Server and push data into tables
#Writes Zindex Scores and Summary  to CandidateZindexScoreDetails and CandidateZindexScore tables
#Inputs are two lists for each of the tables.

import pyodbc
import time
import datetime
from inputParams import InputParamsObj
from debugException import DebugException
import configparser

config = configparser.ConfigParser()
config.read('./common/ConfigFile.properties')

driver=config.get("SQLDatabaseSection", "sql.driver")
server=config.get("SQLDatabaseSection", "sql.server")
port=config.get("SQLDatabaseSection", "sql.port")
tds_version=config.get("SQLDatabaseSection", "sql.tds_version")
uid=config.get("SQLDatabaseSection", "sql.uid")
pwd=config.get("SQLDatabaseSection", "sql.pwd")
database=config.get("SQLDatabaseSection", "sql.database")

### <TODO> configure the connection parameters into properties file later
#conn = pyodbc.connect("DRIVER={FreeTDS};SERVER=QASaturnSQL01.zcdev.local;PORT=1433;TDS_Version=7.2;UID=qatalent;PWD=z2Z*@&k(1(;DATABASE=One")

connString = "DRIVER="+driver+";"+"SERVER="+server+";"+"PORT="+port+";"+"TDS_Version="+tds_version+";"+"UID="+uid+";"+"PWD="+pwd+";"+"DATABASE="+database
conn = pyodbc.connect(connString)

def writeCandidateScoreToSqlServer(reqId, isSubmitted, masterSupplierId, scores, summary):

   currenttime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

   try:
      req_id = reqId
      is_submitted = isSubmitted
      master_supplier_id = masterSupplierId

      cursor = conn.cursor()

      ### First, check if there is any record with this requisition_id
      SQLCmd = ("SELECT RequestID as request_id from  CandidateZindexScore WHERE RequisitionID = ? and IsSubmitted = ? and MasterSupplierId = ? ")
      cursor.execute(SQLCmd, req_id, is_submitted, master_supplier_id)
      recs = cursor.fetchall()
      #print ("Recs Found : %s ", len(recs))
      requestId = 0
      for rec in recs:
         requestId = rec[0]
         #print ("RequestID   : ", requestId)
      if requestId > 0:
         ### If summary already exists, then update it first
         SQLCmd = ("UPDATE  CandidateZindexScore SET ModifiedDateTime = ? WHERE RequestID = ? ")
         cursor.execute(SQLCmd, datetime.datetime.now(), requestId)
      else:
         ### If summary doesn't exist, write the summary into CandidateZindexScore table
         SQLCommand = ("INSERT INTO CandidateZindexScore " \
                       "(RequisitionID, MasterSupplierID, IsSubmitted, IsActive, CreatedDateTime, ModifiedDateTime) " \
                       "VALUES (?,?,?,?,?,?)")
         Values = [reqId, master_supplier_id, is_submitted, 1, datetime.datetime.now(), datetime.datetime.now()]
         cursor.execute(SQLCommand, Values)
      
      ### After summary is written, get the PrimaryKey ID from SQL Server
      SQLCmd=("SELECT TOP 1 RequestID as request_id \
               from  CandidateZindexScore \
               WHERE RequisitionID = ? \
               and IsSubmitted = ? \
               and MasterSupplierId = ? \
               order by ModifiedDateTime desc ")
      cursor.execute(SQLCmd, req_id, is_submitted, master_supplier_id)
      recs = cursor.fetchall()
      #print ("Recs Found : %s ", len(recs))

      summary_id = 0
      for rec in recs:
         summary_id = rec[0]
         #print ("RequestID   : ", summary_id)

      if summary_id <= 0:
         summary_id = -9


      if summary_id > 0:
         ### Check if there are any existing records with the same RequestID
         SQLCmd = ("SELECT count(*) as score_count from  CandidateZindexScoreDetail WHERE RequestID = ? ")
         cursor.execute(SQLCmd, summary_id)
         recs = cursor.fetchall()
         scoreCount = 0
         for rec in recs:
            scoreCount = rec[0]
            ##print ("Detail Count   : ", rec[0])
         if scoreCount > 0:
            ### Now, delete if any details already exists, then delete
            #print("About to delete from CandidateZindexScoreDetail [" + str(scoreCount) + "] documents")
            SQLCmd = ("DELETE from  CandidateZindexScoreDetail WHERE RequestID = ? ")
            cursor.execute(SQLCmd, summary_id)

         ### Now, create score details in CandidateZindexScoreDetail table
         SQLCommand = ("INSERT INTO CandidateZindexScoreDetail " \
                       "(RequestID, ReqCandID, ZindexScore, ZindexDistribution, CreatedDateTime, ModifiedDateTime) " \
                       "VALUES (?,?,?,?,?,?)")
         for score in scores:
            reqId = score["requisition_id"]
            candId = score["candidate_id"]
            zindex = score["zindex_score"]
            zindexDist = str(score["zindex_distribution"])
            Values = [summary_id, candId, zindex, zindexDist, datetime.datetime.now(), datetime.datetime.now()]
            cursor.execute(SQLCommand, Values)

      conn.commit()
      #conn.close()
      return (summary_id)

   except Exception as e:
      DebugException(e)
      return (-9) 


def getPreFilterCandidateList(clientId):

   currenttime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
   beginTime = time.time()
   preFilterCandList = []

   try:

      cursor = conn.cursor()

      SQLCmd = ("select ReqCandID  \
                 from   ReqCandidate rc  \
                 where not exists(select top 1 *   \
                                  from  dnrresource dnr \
                                  where dnr.ClientID = ?  \
                                  and   dnr.UniqueResourceID = rc.UniqueResourceID  \
                                  and   dnr.DNR = 1) \
                 and    rc.OptInTalentSearch = 1  \
                 and    (rc.AllowTalentCloudSearchForAllDivision = 1 or  \
                          (rc.AllowTalentCloudSearchForAllDivision = 0 and  \
	                   exists (select top 1 *    \
                                   from   ReqCandidateDivision rcd  \
                                   where  rcd.Active = 1  \
                                   and    rcd.ReqCandID = rc.ReqCandID \
                                   and    rcd.ClientID= ?)  \
                          )  \
                        )  \
                 and    (rc.mastersupplierid= -1   \
                         or exists(select top 1 *   \
                                   from   zcvendor v   \
                                   where  v.mastersupplierid = rc.MasterSupplierID  \
                                   and    v.clientid= ?   \
                                   and    v.TalentCloudAgreement = 0   \
                                   and    v.StatusID in (2,3))  \
                         or exists(select top 1 *  \
                                   from   zcvendor v  \
                                   where  v.mastersupplierid = rc.MasterSupplierID \
                                   and    v.clientid = ?  \
                                   and    v.TalentCloudAgreement = 1  \
                                   and    v.StatusID in (1,2,3))  \
                         or exists(select top 1 *  \
                                   from   zcvendor v  \
                                   where  v.mastersupplierid = rc.MasterSupplierID  \
                                   and    v.clientid = (SELECT NameValue   \
                                                        from   zcwNameValues  \
                                                        WHERE  Name = 'TalentCloudClientID')  \
                                   and v.StatusID in (2,3))  \
                        ) " )

      bindValues = [clientId, clientId, clientId, clientId]
      cursor.execute(SQLCmd, bindValues)
      recs = cursor.fetchall()
      #print ("Recs Found : %s ", len(recs))

      CandId = 0
      for rec in recs:
         candId = rec[0]
         preFilterCandList.append(candId)

      #print(str(preFilterCandList))

      conn.commit()
      #conn.close()
      print("ClientId [" + str(clientId) + "]   SQL Call elapsed time [" + str(time.time() - beginTime) + "]")
      return (preFilterCandList)

   except Exception as e:
      DebugException(e)
      preFilterCandList = []
      return (preFilterCandList)

