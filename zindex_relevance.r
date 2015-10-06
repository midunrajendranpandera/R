#########################################################################################
#   An R function to calculate the candidates relevance score for Zero Chaos requisitions
#   These R functions are Copyright (C) of Pandera Systems LLP
#########################################################################################


library(rmongodb)
library(plyr)
library(reshape2)
options(warn=-1)
zindex_relevance <- function(ReqId,mongo,res3)
{
        db <- "candidate_model"
    ## Add correct collection where resumes of candidates are parsed
    coll <- "requisition_skills_from_parsed_requisitions"
    ins1 <- paste(db,coll,sep=".")
    ##count <- mongo.count(mongo, ins, mongo.bson.empty())
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisitionId",ReqId)
    query <- mongo.bson.from.buffer(buf)
    cursor <- mongo.find(mongo, ins1, query,,list(requisitionId=1L,parsedWords.word=1L))
    temp <- mongo.cursor.to.list(cursor)
    for(i in 1:length(temp)){
                temp[[i]][1]<-NULL
    }
    temp2<-unlist(temp)
    l<-length(temp2)
    if(l<=1){
                return("No Requisition")
    }
    res<-ldply (temp, data.frame)
    reqskill<-res
    T1<-ncol(reqskill)
    T1<-T1-2
    ##T1<-as.integer(T1/2)
    ##T1<-T1-1
    query<-character()
    for(i in 1:T1){
                c<-paste("parsedWords.word",i,sep=".")
        query[i]<-c
    }
    T1<-"parsedWords.word"
    query<-c(T1,query)
    reqskill<-melt(reqskill,"requisitionId",query,value.name='Skill')
    reqskill <- as.data.frame(reqskill[,3])
    ##Removed Section
        coll <- "_requisition"
        ins <- paste(db,coll,sep=".")
        res<-data.frame()
        buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
    query <- mongo.bson.from.buffer(buf)
    cursor <- mongo.find(mongo, ins, query,,list(requisition_id=1L,job_skill_names.job_skill_name=1L))
    temp <- mongo.cursor.to.list(cursor)
        temp2<-unlist(temp)
    ll<-length(temp2)
        if(ll>2){
                l<-length(temp)
                for(j in 1:l){
                        temp[[j]][1]<-NULL
                }
                temp<-ldply (temp, data.frame)
                temp<-melt(temp,id="requisition_id",value.name='Skill')
                Skill<-data.frame(temp[,3])
                colnames(Skill)<-"reqskill[, 3]"
                reqskill<-rbind(reqskill,Skill)
        }
        ##Appending requisition Skills too above
    Cand<-unique(res3[,"candidateID"])
    Scores<-data.frame(Cand)
    levels<-max(rank(Cand))
    ##For every cadidate
    RScore<-integer()
    for(i in 1:levels){
                Temp <- res3[res3$candidateID==Cand[i],]
        T <- Temp[,2]
        T<- as.data.frame(table(T))
        T <- arrange(T,desc(Freq))
        count <- NROW(T)
        ##T<-as.character(T[,1])
        Check<-reqskill$'reqskill[, 3]' %in% T$T
        div<-0.8*length(Check)
        RScore[i] <- ceiling(40*(sum(Check)/div))
    }
    Scores$RScore <- RScore
    remove(host,db,ins,coll,ins1,count,T,query,cursor,reqskill,res,res3,Cand,levels,i,T,Temp,Tr,PScore)
    return(Scores)
}
