
## For active requisitions
library(rJava)
library(rjson)
source('rm.r')
library(RMongo)
library(data.table)
library(plyr)
source('mongoconnect.r')
source("ss_zindex_main.r")
library(stringdist)
options(warn=-1)
options( java.parameters = "-Xmx5g" )
mongo<-mongoconnect()
reqcoll<-"requisition"
Catmap<-"category_candidate_map"
jobid<-dbGetDistinct(mongo, Catmap, "global_job_category_id")
for(m in 1:length(jobid)){
	keys<-list("req_end_date"=1)
	keys<-append(keys,list("requisition_id"=1))
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	reqjson<-list(global_job_category_id=jobid[m])
	reqjson<-toJSON(reqjson)
	queryout<-dbGetQueryForKeys(mongo, req, reqjson, keys, skip=0,limit=0)
	queryout<-queryout[!duplicated(queryout["requisition_id"]),]
	reqlist<-queryout$requisition_id
	k<-0
	openreq<-integer()
	leng<-nrow(queryout)
	print(paste(paste("Job Id - ",jobid[m]),paste("Total Requisition - ",leng)))
	for(i in 1:leng){
		Temp <- queryout[queryout$requisition_id==reqlist[i],]
		time<-Temp$req_end_date
		time1<-substr(time,1,20)
		t<-substr(time,21,28)
		t<-substr(t,5,8)
		time<-paste(time1,t)
		time<-as.Date(time,format="%a %b %d %T %Y")
		today<-Sys.Date()
		if(time>today){
			k<-k+1
			openreq[k]<-reqlist[i]
		}
	}
	opnreqleng<-length(openreq)
	print(paste(paste("Job Id - ",jobid[m]),paste("Open Requisition - ",opnreqleng)))
}

