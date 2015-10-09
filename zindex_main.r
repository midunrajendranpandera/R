###############################################################################################
#   An R function to calculate the zendex score of candidates for a Zero Chaos requisitions   #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################

library(rmongodb)
library(plyr)
library(reshape2)
library(stringdist)
options(warn=-1)
source("zindex_relevance.r")
source("zindex_probabilistics.r")
source("zindex_experience.r")
source("icc.r")
source("insert_zindex.r")
source("return_zindex.r")

zindex_main<-function(ReqId,Insert='c',...)
{
    host <- 'devmongo01.zcdev.local:27000'
    db <- "candidate_model"
    username<-"candidateuser"
    password<-"bdrH94b9tanQ"
    mongo <- mongo.create(host=host, db= db,username=username,password=password)
    Cand<-c(...)
    Cand<-unlist(Cand)
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
                
		score<-data.frame(Cand)
		score$RScore<-0
		score$PScore<-0
		score$EScore<-0
		ScoresJ<-return_zindex(ReqId,score)
		return(ScoresJ)
    }
	coll <- "ideal_candidate_characteritics"
    idealcoll <- paste(db,coll,sep=".")
    buf <- mongo.bson.buffer.create()
    T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
    query <- mongo.bson.from.buffer(buf)
    count<-mongo.count(mongo,idealcoll,query)
    if(count==0){
                Status<-icc(ReqId)
    }
    ## Getting data from parsed resume##
    coll <- "candidate_skills_from_parsed_resumes"
    ins <- paste(db,coll,sep=".")
    res<-data.frame()
    k<-0
    candnoresume<-integer()
    coll <- "candidate_skills_from_parsed_resumes"
    ins <- paste(db,coll,sep=".")
    res<-data.frame()
    k<-0
    candnoresume<-integer()
	buf <- mongo.bson.buffer.create()
	mongo.bson.buffer.start.array(buf, "$or")
	for(i in 1:length(Cand)){
		mongo.bson.buffer.start.object(buf, toString(i-1))
		# "a":1
		mongo.bson.buffer.append.int(buf, "candidateID", as.integer(Cand[i]))
		# ... }
		mongo.bson.buffer.finish.object(buf)
			
	}
	mongo.bson.buffer.finish.object(buf)
	CandQuery <- mongo.bson.from.buffer(buf)
	cursor <- mongo.find(mongo, ins, CandQuery,,list(candidateID=1L,parsedWords.word=1L, parsedWords.interpreter_value=1L))
	res<- mongo.cursor.to.data.frame(cursor)
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
    CanAllSkill <- melt(res,id="candidateID",query1,value.name='SkillSet')
	CanAllSkill <- CanAllSkill[complete.cases(CanAllSkill),]
	CanAllSkill <- CanAllSkill[,c(1,3)]
	reqskill <- melt(res,id="candidateID",query,value.name='SkillSet')
	reqskill <- reqskill[complete.cases(reqskill),]
	 #res2<-res2[complete.cases(res2),]
    reqskill.sub<-reqskill[with(reqskill,SkillSet=="skills"),]
    ll<-nrow(reqskill.sub)
    if(ll!=0){
        vars<-as.character(reqskill.sub$variable)
        vars<-gsub("interpreter_value","word",vars)
        res2<-res[c("candidateID",vars)]
        res2<-melt(res2,"candidateID",value.name="SkillSet")
        CanSkill<-res2[,c(1,3)]
    }
    if(ll==0){
        CanSkill<-CanAllSkill
	}
	Candd<-unique(CanAllSkill[,"candidateID"])
	CandNoResume<- setdiff(Cand,Candd)
	if(length(CandNoResume)!=0){
		CandNoResume<-data.frame(CandNoResume)
		colnames(CandNoResume)<- 'candidateID'
		res2$SkillSet<-"NA"
		CanSkill<-rbind.fill(CanSkill[colnames(CanSkill)], CandNoResume[colnames(CandNoResume)])
		CanAllSkill<-rbind.fill(CanAllSkill[colnames(CanAllSkill)], CandNoResume[colnames(CandNoResume)])
	}
		
	
	###Getting data from Candidate collection for skills###
    coll <- "candidate"
    ins <- paste(db,coll,sep=".")
    candnoskill<-integer()
    k<-0
    res<-data.frame()	
    for(i in 1:length(Cand)){
        buf <- mongo.bson.buffer.create()
        T <- mongo.bson.buffer.append(buf,"candidate_id",Cand[i])
        query <- mongo.bson.from.buffer(buf)
        cursor <- mongo.find(mongo, ins, query,,list(candidate_id=1L,job_skill_names.job_skill_name=1L))
        temp <- mongo.cursor.to.list(cursor)
        temp2<-unlist(temp)
        l<-length(temp2)
        if(l<=2)
        {
                        k<-k+1
            candnoskill[k]<-as.integer(temp2[2])
            next
        }
        l<-length(temp)
        for(j in 1:l){
                        ##print(i)
            #print(j)
            temp[[j]][1]<-NULL
        }
        temp<-ldply (temp, data.frame)
        temp<-melt(temp,id="candidate_id",value.name='SkillSet')
        temp<-temp[,c(1,3)]
        res <- rbind.fill(res[colnames(res)], temp[colnames(temp)])
    }
	colnames(res)<-c("candidateID","SkillSet")
	CanAllSkill<-rbind(res,CanAllSkill)
	CanSkill<-rbind(res,CanSkill)
    RScore<-zindex_relevance(ReqId,mongo,CanAllSkill)
    if(RScore=="No Requisition"){
                return("Not a valid Requisition; Requisition do not have any requirements")
    }
    PScore<-zindex_probabilistics(ReqId,mongo,CanAllSkill)
    if(PScore=="No Ideal Table"){
                Scores<-RScore
        Scoretemp<-Scores
        Scoretemp$PScore<-0
        PScore<-subset(Scoretemp,select=c(Cand,PScore))
    }
    Scores<-merge(RScore,PScore,by="Cand")
    EScore<-zindex_experience(ReqId,mongo,CanSkill,Cand)
    Scores<-merge(Scores,EScore,by="Cand")
    ##Condition to check insert condition
    if(Insert=='c' | Insert=='C'){
                insert_zindex(ReqId,Scores,mongo)
        return("Scores Inserted")
    }
    else if(Insert=='r' | Insert=='R'){
                ScoresJ<-return_zindex(ReqId,Scores)
        return(ScoresJ)
    }
    else{
                return("Unknown Insert/Return condition")
        }
}
