
library(rJava)
library(rjson)
source('rm.r')
library(RMongo)
library(data.table)
library(plyr)
source('mongoconnect.r')
source("ss_zindex_main.r")
source(return_zindex)
library(stringdist)
options(warn=-1)
options( java.parameters = "-Xmx5g" )
source("scorebatch.r")
mongo<-mongoconnect()
Catmap<-"category_candidate_map"
req<-"requisition"
jobid<-dbGetDistinct(mongo, Catmap, "global_job_category_id")
l<-length(jobid)
dbDisconnect(mongo)
for(m in 1:l){
	mongo<-mongoconnect()
	print(paste("Global Job Id - ",jobid[m]))
	reqjson<-list(global_job_category_id=as.integer(jobid[m]))
	reqjson<-toJSON(reqjson)
	keys<-list("candidates"=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, Catmap, reqjson, keys, skip=0,limit=0)
	CandidatesList<-queryout$candidates
	CandidatesList<-as.integer(fromJSON(CandidatesList))
	CandidatesList<-unique(CandidatesList)
	keys<-list("requisition_id"=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, req, reqjson, keys, skip=0,limit=0)
	RequisitionList<-queryout$requisition_id
	RequisitionList<-unique(RequisitionList)
	dbDisconnect(mongo)
	for(i in 1:length(RequisitionList)){
		Status<-ss_zindex_main(RequisitionList[i],'c',CandidatesList)
		print(status)
	}
}