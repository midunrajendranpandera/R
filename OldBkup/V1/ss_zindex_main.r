
## Zindex main to score candidates for a given requisition.

library(rJava)
library(rjson)
source('rm.r')
library(RMongo)
library(data.table)
library(plyr)
source('mongoconnect.r')
source("zindex_relevance.r")
source("zindex_probabilistics.r")
source("zindex_experience.r")
source("icc.r")
source("ss_insert_zindex.r")
source("return_zindex.r")
library(stringdist)
options(warn=-1)
options( java.parameters = "-Xmx5g" )

ss_zindex_main<-function(ReqId,Insert='c',...)
{
    mongo<-mongoconnect()
    Cand<-c(...)
    Cand<-unlist(Cand)
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
	
	print("Checking ICC")
    idcc <- "ideal_candidate_characteritics"
    
	reqjson<-list(requisition_id=as.integer(ReqId))
	reqjson<-toJSON(reqjson)
	keys<-list("requisition_id"=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	
    queryout<-dbGetQueryForKeys(mongo, reqparse, reqjson, keys, skip=0,limit=0)
	
	test<-queryout[!is.na(queryout)]
	if(test==0){
		Status<-icc(ReqId)
	}
	
    ## Getting data from parsed resume##
    #print(paste("Getting Parsed Data -",Sys.time()))
print("Getting Parsed Skills")
	canparse <- "candidate_skills_from_parsed_resumes"
   
	if(length(Cand)==1){
		candjson<-list(candidate_id=Cand)
		candjson<-toJSON(candjson)
	}
	if(length(Cand)!=1){
		for(i in 1:length(Cand)){
			ll<-list(candidate_id=Cand[i])
			tempjson<-toJSON(ll)
			if(i>1){
				candjson<-c(candjson,tempjson)
			}
			if(i==1){
				candjson<-tempjson
			}
		}
		candjson<-toString(candjson)
		candjson<-paste("{$or:[",candjson)
		candjson<-paste(candjson,"]}")
	}
	keys<-list(candidate_id=1)
	keys<-append(keys,list(parsedWords=1))
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, canparse, candjson, keys, skip=0,limit=0)
	temp<-queryout[!is.na(queryout)]
	
	if(length(temp)==0){
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
	Candd<-unique(queryout[,"candidate_id"])
	CandSkill<-data.frame()
	CandallSkill<-data.frame()
	k<-0
	for(c in 1:length(Candd)){
		Temp <- queryout[queryout$candidate_id==Candd[c],]
		Temp<-Temp$parsedWords
		if(length(Temp)==0) next
		Temp<-fromJSON(Temp)
		if(length(Temp)==0) next
		k<-k+1
		temp<-rbindlist(Temp)
		temp$candidate_id<-Candd[c]
		if(k==1){CandallSkill<-temp}
		if(k>1){
			l<-list(CandallSkill,temp)
			CandallSkill<- rbindlist(l)
		}
	}
		if(nrow(CandallSkill)==0){
		score<-data.frame(Cand)
		score$RScore<-0
		score$PScore<-0
		score$EScore<-0
		if(Insert=='c' | Insert=='C'){
			ss_insert_zindex(ReqId,score,mongo)
			return("Scores Inserted")
		}
		else if(Insert=='r' | Insert=='R'){
			ScoresJ<-return_zindex(ReqId,score)
			return(ScoresJ)
		}
		else{
			return("Unknown Insert/Return condition")
		}
	
	}
	CandSkill<-CandallSkill[CandallSkill$interpreter_value=="skills"]
	CandallSkill<-data.frame(CandallSkill)
	
	CandallSkill<-subset(CandallSkill, select=c("candidate_id","word"))
	CandSkill<-subset(CandSkill, select=c("candidate_id","word"))
	
	Candd<-unique(CandallSkill[,"candidate_id"])
	CandNoResume<-setdiff(Cand,Candd)
	
	if(length(CandNoResume)!=0){
		CandNoResume<-data.frame(CandNoResume)
		colnames(CandNoResume)<- 'candidate_id'
		CandNoResume$word<-"NA"
		CandallSkill<-rbind.fill(CandallSkill[colnames(CandallSkill)], CandNoResume[colnames(CandNoResume)])
	}
    ###Getting data from Candidate collection for skills###
print("Getting Skills from Candidate Collection")

candcoll <- "candidate"
	keys<-list(candidate_id=1)
	keys<-append(keys,list(job_skill_names=1))
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, candcoll, candjson, keys, skip=0,limit=0)
	temp<-queryout[!is.na(queryout)]
	skill<-data.frame()
	skilldata<-character()
	if(length(temp)!=0){
		queryout<-queryout[,c(1,2)]
		Candd<-unique(queryout[,"candidate_id"])
		k<-0
		for(c in 1:length(Candd)){
			Temp <- queryout[queryout$candidate_id==Candd[c],]
			Temp<-Temp$job_skill_names
			len<-length(Temp)
			if(len==0) next
			Temp<-fromJSON(Temp)
			len<-length(Temp)
			if(len==0) next
			k<-k+1
			##Once NULL datatype is removed, change below code use rbindlist which is efficient
			temp<-unlist(Temp)
			t<- ceiling(length(temp))/ceiling(length(temp)/5)
			tt<- ceiling(length(temp)/5)
			l<-2
			for(j in 1:tt){
				skilldata[j]<-as.character(temp[l])
				l<-l+t
			}
			temp<-data.frame(skilldata)
			colnames(temp)<-"word"
			temp$candidate_id<-Candd[c]
			if(k==1){skill<-temp}
			if(k>1){
				l<-list(skill,temp)
				skill<- rbindlist(l)
			}
		}
		if(nrow(skill)!=0){
			skill<-data.frame(skill)
			skill<-skill[,c(2,1)]
			CandallSkill<-rbind.fill(CandallSkill[colnames(CandallSkill)], skill[colnames(skill)])
			CandSkill<-rbind.fill(CandSkill, skill)
		
		}
	}
	CandallSkill<-data.frame(CandallSkill)
	CandSkill<-data.frame(CandSkill)


print("Before RScore")
    Scores<-data.frame()
	RScore<-zindex_relevance(ReqId,mongo,CandallSkill,reqallskill)
    
print("Before PScore")
    PScore<-zindex_probabilistics(ReqId,mongo,CandallSkill)
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

