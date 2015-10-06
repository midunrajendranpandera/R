###############################################################################################
#   An R function to calculate the zendex score of candidates for a Zero Chaos requisitions   #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################

library(rmongodb)
library(plyr)
library(reshape2)
options(warn=-1)
source("zindex_relevance.r")
source("zindex_probabilistics.r")
source("zindex_experience.r")
#source("icc.r")
#source("insert_zindex.r")

zindex_main<-function(ReqId,Insert='c',...)
{
    ## Function that creates the Ideal Charact Table
    ## If not created
    ##icc(ReqId)
    ## Function that calculates the relevance score
    ##RScores<-zendex_relevance(ReqId)
    ## Function that calculates the experience score
    ##EScores<-zendex_experience(ReqId)
    ## Function that calculate the probabilistics score

    host <- 'devmongo01.zcdev.local:27000'
    db <- "candidate_model"
    username<-"candidateuser"
    password<-"bdrH94b9tanQ"
    mongo <- mongo.create(host=host, db= db,username=username,password=password)
    Cand<-c(...)
	## Getting data from parsed resume##
    coll <- "candidate_skills_from_parsed_resumes"
    ins <- paste(db,coll,sep=".")
    res<-data.frame()
    k<-0
    candnoresume<-integer()
    for(i in 1:length(Cand)){
        buf <- mongo.bson.buffer.create()
        T <- mongo.bson.buffer.append(buf,"candidateID",Cand[i])
        query <- mongo.bson.from.buffer(buf)
        cursor <- mongo.find(mongo, ins, query,,list(candidateID=1L,parsedWords.word=1L, parsedWords.count=1L))
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
    res2<-res2[,c(1,3)]
    if(length(candnoresume)>=1){
                candnoresume <- data.frame(candnoresume)
                colnames(candnoresume)<- 'candidateID'
                res2<-rbind.fill(res2[colnames(res2)], candnoresume[colnames(candnoresume)])
    }
	res2<-res2[complete.cases(res2),]
    ###Getting data from Candidate collection for skills###
	coll <- "_candidate"
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
    if(nrow(res)!=0){
                colnames(res)<-c("candidateID","SkillSet")
        if(length(candnoskill)>=1){
                        candnoskill <- data.frame(candnoskill)
            colnames(candnoskill)<- 'candidateID'
            res<-rbind.fill(res[colnames(res)], candnoskill[colnames(candnoskill)])
        }
    }
    if(nrow(res)==0){
                candnoskill <- data.frame(candnoskill)
        candnoskill$Skillset<-NA
        res<-rbind.fill(res[colnames(res)], candnoskill[colnames(candnoskill)])
        colnames(res)<-c("candidateID","SkillSet")
    }
    res2<-rbind(res2,res)
    #res2<-res2[complete.cases(res2),]
	candskill<-res
    PScore<-zindex_probabilistics(ReqId,mongo,res2)
    RScore<-zindex_relevance(ReqId,mongo,res2)
    if(RScore=="No Requisition"){
		return("Not a valid Requisition; Requisition do not have any requirements")
    }
    Scores<-merge(RScore,PScore,by="Cand")
	EScore<-zindex_experience(ReqId,mongo,candskill)
	Scores<-merge(Scores,EScore,by="Cand")

    #Scores<-merge(Scores,EScore,by="Cand")
    #coll<-"requisition_candidate"
    ####ins <- paste(db,coll,sep=".")
    #buf <- mongo.bson.buffer.create()
    #T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
    ##query <- mongo.bson.from.buffer(buf)
    #cursor <- mongo.find.one(mongo, ins, query,list(data_center=1L))
    #datacenter<-mongo.bson.to.list(cursor)
    ##datacenter<-datacenter$data_center
    #T<-mongo.disconnect(mongo)
    #insert_zindex(ReqId,Scores,datacenter)
    #return(Scores)
	
	##If condition to check insert condition

    return(Scores)
}
