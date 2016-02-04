###############################################################################################
#   An R function to calculate the candidates probabilistics score for Zero Chaos requisitions#
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################


zindex_experience <- function(ReqId,mongo,candskill,Cand)
{
    ##Getting skills data from Requisition parsed collection##################
    db <- "candidate_model"
    coll <- "requisition_skills_from_parsed_requisitions"
    ins1 <- paste(db,coll,sep=".")
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisitionId",ReqId)
    query <- mongo.bson.from.buffer(buf)
    cursor <- mongo.find(mongo, ins1, query,,list(requisitionId=1L,parsedWords.interpreter_value=1L,parsedWords.word=1L))
    res <- mongo.cursor.to.data.frame(cursor)
    T1<-ncol(res)
    T1<-T1-1
    T1<-T1/2
    T1<-T1-1
    query1<-character()
    query2<-character()
    for(i in 1:T1){
                c<-paste("parsedWords.word",i,sep=".")
        query1[i]<-c
        c<-paste("parsedWords.interpreter_value",i,sep=".")
        query2[i]<-c
    }
    T1<-"parsedWords.word"
    query1<-c(T1,query1)
    T1<-"parsedWords.interpreter_value"
    query2<-c(T1,query2)
    query<-c(query1,query2)
    reqskill <- melt(res,id="requisitionId",query,value.name='SkillSet')
    reqskill.sub<-reqskill[with(reqskill,SkillSet=="skills"),]
    ll<-nrow(reqskill.sub)
    if(ll!=0){
                vars<-as.character(reqskill.sub$variable)
        vars<-gsub("interpreter_value","word",vars)
        res2<-res[c("requisitionId",vars)]
        res2<-melt(res2,"requisitionId",value.name="SkillSet")
        reqs<-as.character(res2[,3])
        reqskill<-reqs
    }
    if(ll==0){
                reqskill<-melt(res,"requisitionId",query1,value.name="SkillSet")
        reqskill<-as.character(reqskill[,3])
    }
    ##Requisitions that are marked/parsed as Skills - saved as reqskill ######
    ##Getting skill data from candidate parsed collection#####################
    ##$$$Removed Code$$$$$
    ##Candidate Skills from resume & candidate collection - saved as candskill#
    Cand<-unique(candskill[,"candidateID"])
    Scores<-data.frame(Cand)
    levels<-length(Cand)
    EScore<-integer()
    for(i in 1:levels){
                Temp <- candskill[candskill$candidateID==Cand[i],]
        Temp<-Temp[complete.cases(Temp),]
        T <- Temp[,2]
        T<- as.data.frame(table(T))
        T<-as.character(T[,1])
        len<-length(T)
        if(len==0) {
                        EScore[i]<-0
                        next
        }
        Value<-0
        for(j in 1:length(reqskill)){
                        Check<- stringdist(reqskill[j],T,method='soundex')
            if(sum(Check)<len){
                                Value<-Value+1
            }
        }
        if(Value==0){
                        EScore[i]<-0
        }
        else{
                        EScore[i] <- ceiling((Value*40)/length(reqskill))
        }
    }
    Scores$EScore <- EScore
    remove(coll,ins1,buf,T,query,cursor,res,T1,query1,query2,i,j,reqskill,vars,res2,reqs,reqskill,ins,res,k,temp,candnoresume,res2,res2.sub,reqskill.sub,res3,len,candskill,levels,Temp,EScore)
    return(Scores)
}
