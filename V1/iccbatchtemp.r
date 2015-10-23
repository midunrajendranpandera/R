
library(rJava)
library(rjson)
source('rm.r')
library(RMongo)
library(data.table)
library(plyr)
source('icc_bat.r')
source('mongoconnect.r')
options(warn=-1)
options( java.parameters = "-Xmx2g" )
mongo<-mongoconnect()
req<-"requisition"
idcc <- "ideal_candidate_characteritics"
reqcan<-"requisition_candidate"
requisitionList<-dbGetDistinct(mongo, req, "requisition_id")
l<-length(requisitionList)
for(m in 80032:l){
        print(paste("Requisition Index - ",m))
        print(paste("Requisition - ",requisitionList[m]))
        print(paste("Start Time - ",Sys.time()))
		reqjson<-list(requisition_id=as.integer(requisitionList[m]))
		reqjson<-toJSON(reqjson)
		keys<-list("global_job_category_id"=1)
		keys<-append(keys,list("requisition_id"=1))
		keys<-append(keys,list("_id"=0))
		keys<-toJSON(keys)
		queryout<-dbGetQueryForKeys(mongo, req, reqjson, keys, skip=0,limit=0)
		jobid<-unique(queryout$global_job_category_id)
		jobid<-as.integer(jobid)
		reqjson<-list(requisition_id=requisitionList[m])
		reqjson<-toJSON(reqjson)
		keys<-list("requisition_id"=1)
		keys<-append(keys,list("_id"=0))
		keys<-toJSON(keys)
		queryout<-dbGetQueryForKeys(mongo, idcc, reqjson, keys, skip=0,limit=0)
		queryout<-queryout[!is.na(queryout)]
		if(length(queryout)!=0){
			print("Skipping")
            next
		}
        print("Calling ICC")
        status<-icc_bat(requisitionList[m],mongo)
        print(status)
        print(paste("Ending Time - ",Sys.time()))
}
print("Done")

