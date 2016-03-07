library(rmongodb)
library(plyr)
library(reshape2)
library(stringdist)
options(warn=-1)
source("zindex_relevance.r")
source("zindex_probabilistics.r")
source("zindex_experience.r")
source("icc.r")
source("ss_insert_zindex.r")
source("return_zindex.r")
scorebatch<-function(ReqId,mongo,CandidatesList,CandSkill,CandAllSkill){

#print("Inside Score Batch")
	coll <- "requisition_skills_from_parsed_requisitions"
    ins1 <- paste(db,coll,sep=".")
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisitionId",ReqId)
    query <- mongo.bson.from.buffer(buf)
    cursor <- mongo.find(mongo, ins1, query,,list(requisitionId=1L,parsedWords.word=1L))
    temp <- mongo.cursor.to.list(cursor)
    temp2<-unlist(temp)
    l<-length(temp2)
    if(l<=1 | length(temp)==0){
		score<-data.frame(CandidatesList)
		score$RScore<-0
		score$PScore<-0
		score$EScore<-0
		ss_insert_zindex(ReqId,score,mongo)
		return("Scores Inserted")
    }
#print("Before RScore")
	RScore<-zindex_relevance(ReqId,mongo,CandAllSkill)
    if(RScore=="No Requisition"){
		score<-data.frame(CandidatesList)
        score$RScore<-0
        score$PScore<-0
        score$EScore<-0
		ss_insert_zindex(ReqId,score,mongo)
		return("Scores Inserted")
    }
#print("Before PScore")
    PScore<-zindex_probabilistics(ReqId,mongo,CandAllSkill)
    if(PScore=="No Ideal Table"){
        Scores<-RScore
        Scoretemp<-Scores
        Scoretemp$PScore<-0
        PScore<-subset(Scoretemp,select=c(CandidatesList,PScore))
    }
#print("Scores Merging")
    Scores<-merge(RScore,PScore,by="Cand")
#print("Before EScore")
    EScore<-zindex_experience(ReqId,mongo,CandSkill,CandidatesList)
#print("After ESCore and Before Merging")
    Scores<-merge(Scores,EScore,by="Cand")
    ##Condition to check insert condition
#print("Before SS Insert")
	ss_insert_zindex(ReqId,Scores,mongo)
	return("Scores Inserted")
}

