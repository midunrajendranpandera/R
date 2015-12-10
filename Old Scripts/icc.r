###############################################################################################
#   An R function to create ideal characteristics table for Zero Chaos requisitions           #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################

library(rmongodb)
library(plyr)
library(reshape2)
options(warn=-1)
source("icc_candilist.r")

icc <- function(ReqId)
{
    host <- 'devmongo01.zcdev.local:27000'
    db <- "candidate_model"
    username<-"candidateuser"
    password<-"bdrH94b9tanQ"
    mongo <- mongo.create(host=host, db= db,username=username,password=password)
        coll <- "candidate_skills_from_parsed_resumes"
    ins <- paste(db,coll,sep=".")
    coll <- "ideal_candidate_characteritics"
    ons <- paste(db,coll,sep=".")
    coll<-"requisition"
    rcc <- paste(db,coll,sep=".")
    CandidateList <- icc_candilist(ReqId,mongo)
    if(CandidateList=="No Relevant Profile Available"){
                buf <- mongo.bson.buffer.create()
                T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
                query <- mongo.bson.from.buffer(buf)
                cursor <- mongo.find.one(mongo, rcc, query,list(data_center=1L))
                data_center<-mongo.bson.to.list(cursor)
                data_center<-data_center$data_center
                buf <- mongo.bson.buffer.create()
                T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
                query <- mongo.bson.from.buffer(buf)
                count <- mongo.count(mongo, ons, query)
                if(count>=1) {T<-mongo.remove(mongo,ons,query)}
                ReqId<-as.integer(ReqId)
                Out <- mongo.bson.from.list(list(requisition_id=ReqId,Region=data_center,Skills="No Skills"))
                Insert <- mongo.insert(mongo,ons,Out)
                ok <- mongo.disconnect(mongo)
        return("No Relevant Profile Available; Unable to create Ideal Characteristics Table")
    }

    count <- mongo.count(mongo, ins, mongo.bson.empty())
    k<-0
        res<-data.frame()
    for(i in 1:length(CandidateList)){
                buf <- mongo.bson.buffer.create()
        T <- mongo.bson.buffer.append(buf,"candidateID",CandidateList[i])
        query <- mongo.bson.from.buffer(buf)
        cursor <- mongo.find(mongo, ins, query,,list(candidateID=1L,parsedWords.word=1L, parsedWords.count=1L))
        temp <- mongo.cursor.to.list(cursor)
                temp2<-unlist(temp)
        if(length(temp2)<=2){next}
        else{
                        k<-k+1
            for(j in 1:length(temp)){
                temp[[j]][1]<-NULL
            }
            temp<-ldply (temp, data.frame)
            if(k==1){
                res<-temp
            }
            else{
                res <- rbind.fill(res[colnames(res)], temp[colnames(temp)])
            }
        }
    }
        if(k==0){return("No Relevant Profile Available; Unable to create Ideal Characteristics Table")}
    T1<-ncol(res)
    T1<-T1-1
    T1<-as.integer(T1/2)
    T1<-T1-1
    query<-character()
    for(i in 1:T1){
                c<-paste("parsedWords.word",i,sep=".")
        query[i]<-c
    }
    T1<-"parsedWords.word"
    query<-c(T1,query)
    res2 <- melt(res,"candidateID",query,value.name='SkillSet')
    res2<-res2[,3]
    res<-res2[!is.na(res2)]
    T<- as.data.frame(table(res))
    T <- arrange(T,desc(Freq))
    T<-as.character(T[,1])
    count <- NROW(T)
    i<-1:count
    x3 <- as.integer(quantile(i,c(.33,.66,1)))
    k=1
    Skills <- character()
    for (j in 1:x3[1]){
                Skills[k]<- T[j]
        k<-k+1
    }
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
    query <- mongo.bson.from.buffer(buf)
    cursor <- mongo.find.one(mongo, rcc, query,list(data_center=1L))
    data_center<-mongo.bson.to.list(cursor)
    data_center<-data_center$data_center
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
    query <- mongo.bson.from.buffer(buf)
    count <- mongo.count(mongo, ons, query)
    if(count>=1) {T<-mongo.remove(mongo,ons,query)}
    ReqId<-as.integer(ReqId)
    Out <- mongo.bson.from.list(list(requisition_id=ReqId,Region=data_center,Skills=Skills))
    Insert <- mongo.insert(mongo,ons,Out)
    ok <- mongo.disconnect(mongo)
        return("Success")
    remove(host,db,mongo,coll,ins,ons,buf,cursor,query,res,T1,res2,T,Skills,Out,ok,i,j,k)
}
