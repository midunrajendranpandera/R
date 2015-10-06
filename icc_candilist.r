###############################################################################################
#   An R function to retreive relevant candidates for a requisition                           #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################

library(rmongodb)
library(plyr)
options(warn=-1)
source('candilist.r')

icc_candilist <- function(ReqId,mongo)
{
    coll<-"_requisition_candidate"
    db <- "candidate_model"
    ins1 <- paste(db,coll,sep=".")
    coll<-"_requisition"
    ins <- paste(db,coll,sep=".")
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
    query <- mongo.bson.from.buffer(buf)
    cursor <- mongo.find(mongo, ins, query,,list(global_job_category_id=1L))
    jobid<-mongo.cursor.to.data.frame(cursor)
    jobid<-as.integer(jobid[,1])
    jobid<-unique(jobid)
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"global_job_category_id",jobid)
    query <- mongo.bson.from.buffer(buf)
    cursor <- mongo.find(mongo, ins, query,,list(requisition_id=1L))
    reqid<-mongo.cursor.to.data.frame(cursor)
    reqid<-as.integer(reqid[,1])
    reqid<-unique(reqid)
    k<-0
    candidateid<-integer()
    for( i in 1:length(reqid)){
                buf <- mongo.bson.buffer.create()
        T <- mongo.bson.buffer.append(buf,"requisition_id",reqid[i])
        T <- mongo.bson.buffer.append(buf,"is_hired",1)
        query <- mongo.bson.from.buffer(buf)
        count <- mongo.count(mongo, ins1, query)
        if(count>=1){
                        cursor <- mongo.find(mongo, ins1, query,,list(candidate_id=1L))
            candid<-mongo.cursor.to.data.frame(cursor)
            candid<-as.integer(candid[,1])
            candid<-unique(candid)
            k<-k+1
            candidateid[k]<-candid
        }
        else next
    }
    candidateid<-candidateid[!is.na(candidateid)]
    if (k==0){
                candidateid <- candilist(ReqId,mongo)
    }
	if(candidateid=="No Candidates"){
		remove(host,db,ins,coll,query,cursor,ins1,candid,k,count,jobid,reqid,i,T,candid,buf,k)
		return("No Relevant Profile Available")
	}
    remove(host,db,ins,coll,query,cursor,ins1,candid,k,count,jobid,reqid,i,T,candid,buf,k)
    return(candidateid)
}
