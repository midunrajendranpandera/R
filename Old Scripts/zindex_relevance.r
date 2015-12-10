###############################################################################################
#   An R function to calculate the candidates relevance score for Zero Chaos requisitions     #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################


zindex_relevance <- function(ReqId,mongo,res3)
{
    db <- "candidate_model"
    coll <- "requisition_skills_from_parsed_requisitions"
    ins1 <- paste(db,coll,sep=".")
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
    query<-character()
        if(T1!=0){
                for(i in 1:T1){
                        c<-paste("parsedWords.word",i,sep=".")
                        query[i]<-c
                }
        }
    T1<-"parsedWords.word"
    query<-c(T1,query)
    reqskill<-melt(reqskill,"requisitionId",query,value.name='Skill')
    reqskill <- as.data.frame(reqskill[,3])
    coll <- "requisition"
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
    Cand<-unique(res3[,"candidateID"])
    Scores<-data.frame(Cand)
    levels<-max(rank(Cand))
    RScore<-integer()
    for(i in 1:levels){
        Temp <- res3[res3$candidateID==Cand[i],]
        T <- Temp[,2]
        T<- as.data.frame(table(T))
        T <- arrange(T,desc(Freq))
        count <- NROW(T)
        Check<-reqskill$'reqskill[, 3]' %in% T$T
        div<-0.8*length(Check)
        RScore[i] <- ceiling(40*(sum(Check)/div))
    }
    Scores$RScore <- RScore
    remove(db,coll,ins1,buf,query,T,cursor,temp,i,temp2,l,res,reqskill,T1,ins,llj,Skill,Cand,levels,RScore,Temp,count,Check,div)
    return(Scores)
}
