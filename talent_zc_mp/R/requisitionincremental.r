
## New Requisition Incremental -- nohup R CMD BATCH requisitionincremental.r requisitionincrementallog.out &
## Creates the ideal characteristics table for new requisitions

library(rJava)
library(rjson)
source('rm.r')
library(RMongo)
library(data.table)
library(plyr)
source('mongoconnect.r')
library(stringdist)
options(warn=-1)
source('icc.r')
options( java.parameters = "-Xmx5g" )

mongo<-mongoconnect()
req<-"requisition"
idcc<-"ideal_candidate_characteritics"
reqlist<-dbGetDistinct(mongo, req, "requisition_id")
idccreq<-dbGetDistinct(mongo, idcc, "requisition_id")
requisitionlist<-setdiff(reqlist,idccreq)
l<-length(requisitionlist)
dbDisconnect(mongo)
for(m in 1:l){
	t<-t+1
	print(m)
	print(requisitionlist[m])
	Status<-icc(requisitionlist[m])
	print(Status)
}

