###############################################################################################
#   An R function to calculate the candidates probabilistics score for Zero Chaos requisitions
#   These R functions are Copyright (C) of Pandera Systems LLP
###############################################################################################

library(rmongodb)
library(plyr)
library(reshape2)
options(warn=-1)

zindex_probabilistics <- function(ReqId,mongo,res3)
{
    db <- "candidate_model"
    coll <- "ideal_candidate_characteritics"
    ins1 <- paste(db,coll,sep=".")
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
    query <- mongo.bson.from.buffer(buf)
    cursor <- mongo.find(mongo, ins1, query,,list(Skills=1L))
    res <- mongo.cursor.to.list(cursor)
    for(i in 1:length(res)){
                res[[i]][1]<-NULL
    }
    res<-ldply (res, data.frame)
    idcskill <- as.data.frame(res[,1])
    colnames(idcskill)<-"Skill"
    Cand<-unique(res3[,"candidateID"])
    Scores<-data.frame(Cand)
    levels<-max(rank(Cand))
    PScore<-integer()
    for(i in 1:levels){
		Temp <- res3[res3$candidateID==Cand[i],]
        T <- Temp[,2]
        T<- as.data.frame(table(T))
        T <- arrange(T,desc(Freq))
        count <- NROW(T)
        Check<-idcskill$'Skill' %in% T$T
        PScore[i] <- ceiling(20*(sum(Check)/(0.2*length(Check))))
        if(PScore[i] >20) PScore[i]<-20
    }
    Scores$PScore <- PScore
    remove(db,coll,ins1,buf,T,query,cursor,res,i,res,idcskill,Cand,levels,PScore)
    return(Scores)
}
