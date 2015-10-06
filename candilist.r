###############################################################################################
#   An R function to retreive relevant candidates for a requisition                           #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################

library(rmongodb)
library(plyr)
options(warn=-1)

candilist <- function(ReqId,mongo)
{
    db <- "candidate_model"
    coll<-"_requisition_candidate"
    ins <- paste(db,coll,sep=".")
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
    query <- mongo.bson.from.buffer(buf)
    cursor <- mongo.find(mongo, ins, query,,list(candidate_id=1L))
    candidateid<-mongo.cursor.to.data.frame(cursor)
    T<-nrow(candidateid)
	if(T==0){
		remove(host,db,ins,coll,query,cursor)
		return("No Candidates")	
	}
	candidateid<-as.integer(candidateid[,1])
    candidateid<-unique(candidateid)
    remove(host,db,ins,coll,query,cursor)
    return(candidateid)
}
