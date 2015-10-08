###############################################################################################
#   An R function to calculate the candidates probabilistics score for Zero Chaos requisitions#
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################


zindex_experience <- function(ReqId,mongo,canskill,Cand)
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
    coll <- "candidate_skills_from_parsed_resumes"
    ins <- paste(db,coll,sep=".")
    res<-data.frame()
    k<-0
    candnoresume<-integer()
    for(i in 1:length(Cand)){
		buf <- mongo.bson.buffer.create()
        T <- mongo.bson.buffer.append(buf,"candidateID",Cand[i])
        query <- mongo.bson.from.buffer(buf)
        cursor <- mongo.find(mongo, ins, query,,list(candidateID=1L,parsedWords.word=1L, parsedWords.interpreter_value=1L))
        temp <- mongo.cursor.to.list(cursor)
        l<-length(temp)
        if(l==0){
                        k<-k+1
                        candnoresume[k]<-Cand[i]
            next
        }
        for(j in 1:l){
                        temp[[j]][1]<-NULL
        }
                temp<-ldply (temp, data.frame)
                res <- rbind.fill(res[colnames(res)], temp[colnames(temp)])
    }
	canlen<-length(res)
	res2<-data.frame()
	if(canlen!=0){
	
	T1<-ncol(res)
    T1<-T1-1
    T1<-as.integer(T1/2)
    T1<-T1-1
    query1<-character()
        query2<-character()
        query<-character()
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
    res2 <- melt(res,"candidateID",query,value.name='SkillSet')
    res2.sub<-res2[with(res2,SkillSet=="skills"),]
    res2.sub<-res2.sub[complete.cases(res2.sub),]
    vars<-as.character(res2.sub$variable)
    vars<-gsub("interpreter_value","word",vars)
    res3<-res[c("candidateID",vars)]
    res3<-melt(res3,"candidateID",value.name='SkillSet')
    res3<-res3[,c(1,3)]
    res3<-res3[complete.cases(res3),]
	
	}
	if(canlen==0){
		res3<-data.frame(Cand)
		res3$SkillSet<-NA
		colnames(res3)<-c("candidateID","SkillSet")
	}
    
    ##Candidate resume words that are marked/parsed as Skills - saved as res3#
    candskill<-rbind(res3,canskill)
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
