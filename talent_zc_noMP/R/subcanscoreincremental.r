
## Submited candidate incremental scoring

library(rJava)
library(rjson)
source('rm.r')
library(RMongo)
library(data.table)
library(plyr)
source('mongoconnect.r')
library(stringdist)
options(warn=-1)
source('zindex_main.r')
source('candilist.r')
options( java.parameters = "-Xmx5g" )
mongo<-mongoconnect()
reqcan<-"requisition_candidate"
reqlist<-dbGetDistinct(mongo, reqcan, "requisition_id")
subscore<-"requisition_cand_zindex_scores"
scorereq<-dbGetDistinct(mongo, subscore, "requisition_id")
requisitionlist<-setdiff(reqlist,scorereq)
l<-length(requisitionlist)
print(l)
t<-0
for(m in 1:l){
	t<-t+1
	print(m)
	print(requisitionlist[m])
	candidateid<-candilist(requisitionlist[m],mongo)
	if(candidateid=="No Candidates"){
		print(paste("No Submitted Candidates for Req - ",requisitionlist[m]))
		next
	}
	Status<-zindex_main(requisitionlist[m],'c',candidateid)
	print(Status)
	 if(t==25){
                print(t)
                t<-0
                dbDisconnect(mongo)
                mongo<-mongoconnect()
        }

}

