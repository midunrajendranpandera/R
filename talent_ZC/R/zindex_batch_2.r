library(rJava)
library(rjson)
source('rm.r')
library(RMongo)
library(data.table)
library(plyr)
source('mongoconnect.r')
library(stringdist)
options(warn=-1)
options( java.parameters = "-XX:-UseConcMarkSweepGC" )
source('zindex_main.r')
source('candilist.r')
mongo<-mongoconnect()
reqcan<-"requisition_candidate"
requisitionList<-dbGetDistinct(mongo, reqcan, "requisition_id")
l<-length(requisitionList)
ll<-ceiling(l/2)
t<-0
for(m in ll:l){
	t<-t+1
	print(m)
	print(requisitionList[m])
	candidateid<-candilist(requisitionList[m],mongo)
	if(candidateid=="No Candidates"){
		print(paste("No Submitted Candidates for Req - ",requisitionList[m]))
		next
	}
	Status<-zindex_main(requisitionList[m],'c',candidateid)
	print(Status)
	if(t==50){
		print(t)
		t<-0
		dbDisconnect(mongo)
		mongo<-mongoconnect()
	}

}

