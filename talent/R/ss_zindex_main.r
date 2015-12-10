
## Zindex main to score candidates for a given requisition.

library(rJava)
library(rjson)
source('rm.r')
library(RMongo)
library(data.table)
library(plyr)
source('mongoconnect.r')
source("zindex_relevance.r")
source("ss_zindex_probabilistics.r")
source("zindex_experience.r")
source("icc.r")
source("ss_insert_zindex.r")
source("return_zindex.r")
library(stringdist)
options(warn=-1)
options( java.parameters = "-Xmx5g" )

ss_zindex_main<-function(ReqId,Insert='c',Cand,CandallSkill,CandSkill,idcskill)
{
    mongo<-mongoconnect()
  
print("Getting Requisition data")
	reqparse <- "requisition_skills_from_parsed_requisition"
   
	reqjson<-list(requisition_id=as.integer(ReqId))
	reqjson<-toJSON(reqjson)
	
	keys<-list("requisition_id"=1)
	keys<-append(keys,list("parsedWords"=1))
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	
	queryout<-dbGetQueryForKeys(mongo, reqparse, reqjson, keys, skip=0,limit=0)
	
	test<-queryout[!is.na(queryout)]
	
	if(length(test)==0){
		score<-data.frame(Cand)
		score$RScore<-0
		score$PScore<-0
		score$EScore<-0
		if(Insert=='c' | Insert=='C'){
			ss_insert_zindex(ReqId,score,mongo)
			dbDisconnect(mongo)
			return("Scores Inserted")
		}
		else if(Insert=='r' | Insert=='R'){
			ScoresJ<-return_zindex(ReqId,score)
			dbDisconnect(mongo)
			return(ScoresJ)
		}
		else{
			dbDisconnect(mongo)
			return("Unknown Insert/Return condition")
		}	
	}
	test<-queryout$parsedWords
	if(test=="[ ]"){
		score<-data.frame(Cand)
		score$RScore<-0
		score$PScore<-0
		score$EScore<-0
		if(Insert=='c' | Insert=='C'){
			ss_insert_zindex(ReqId,score,mongo)
			dbDisconnect(mongo)
			return("Scores Inserted")
		}
		else if(Insert=='r' | Insert=='R'){
			ScoresJ<-return_zindex(ReqId,score)
			dbDisconnect(mongo)
			return(ScoresJ)
		}
		else{
			dbDisconnect(mongo)
			return("Unknown Insert/Return condition")
		}	
	}
		
	queryout<-queryout[,c(1,2)]
	reqs<-unique(queryout[,"requisition_id"])
	
	Temp <- queryout[queryout$requisition_id==reqs,]
	Temp<-Temp$parsedWords
	Temp<-fromJSON(Temp)
	reqallskill<-rbindlist(Temp)
	reqskill<-reqallskill[reqallskill$interpreter_value=="skills"]
	if(nrow(reqskill)==0){
		reqskill<-reqallskill
	}
	reqallskill<-data.frame(reqallskill)
	reqskill<-data.frame(reqskill)
	reqallskill<-subset(reqallskill, select=c("word"))
	reqskill<-subset(reqskill, select=c("word"))

	if(nrow(reqallskill)==0){
		score<-data.frame(Cand)
		score$RScore<-0
		score$PScore<-0
		score$EScore<-0
		if(Insert=='c' | Insert=='C'){
			ss_insert_zindex(ReqId,score,mongo)
			dbDisconnect(mongo)
			return("Scores Inserted")
		}
		else if(Insert=='r' | Insert=='R'){
			ScoresJ<-return_zindex(ReqId,score)
			dbDisconnect(mongo)
			return(ScoresJ)
		}
		else{
			dbDisconnect(mongo)
			return("Unknown Insert/Return condition")
		}
	
	}
	
	
	
print("Before RScore")
    Scores<-data.frame()
	RScore<-zindex_relevance(ReqId,mongo,CandallSkill,reqallskill)
    
print("Before PScore")
    PScore<-ss_zindex_probabilistics(ReqId,mongo,CandallSkill,idcskill)
    if(PScore=="No Ideal Table"){
        Scores<-RScore
        Scoretemp<-Scores
        Scoretemp$PScore<-0
        PScore<-subset(Scoretemp,select=c(Cand,PScore))
    }
    Scores<-merge(RScore,PScore,by="Cand")
print("Before EScore")
    EScore<-zindex_experience(ReqId,mongo,CandSkill,reqskill,Cand)
print("After EScore")
    Scores<-merge(Scores,EScore,by="Cand")
    ##Condition to check insert condition
    if(Insert=='c' | Insert=='C'){
		ss_insert_zindex(ReqId,Scores,mongo)
		dbDisconnect(mongo)
	        return("Scores Inserted")
    }
    else if(Insert=='r' | Insert=='R'){
		ScoresJ<-return_zindex(ReqId,Scores)
		dbDisconnect(mongo)
	        return(ScoresJ)
    }
    else{
		dbDisconnect(mongo)
		return("Unknown Insert/Return condition")
    }
}

