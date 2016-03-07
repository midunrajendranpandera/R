###############################################################################################
#   An R function to create ideal characteristics table for Zero Chaos requisitions           #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################

options(warn=-1)
options( java.parameters = "-Xmx2g" )
source("icc_candilist.r")
icc_bat <- function(ReqId,mongo)
{
	canparse <- "candidate_skills_from_parsed_resumes"
	idcc <- "ideal_candidate_characteritics"
	req <- "requisition"
	reqcan<-"requisition_candidate"
	ReqId<-as.integer(ReqId)
	reqjson<-list(requisition_id=ReqId)
	reqjson<-toJSON(reqjson)
	keys<-list("data_center"=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, req, reqjson, keys, skip=0,limit=0)
	data_center<-unique(queryout$data_center)
	data_center<-toString(data_center)
	reqjson<-list(requisition_id=ReqId)
	reqjson<-toJSON(reqjson)
	keys<-list("global_job_category_id"=1)
	keys<-append(keys,list("requisition_id"=1))
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, req, reqjson, keys, skip=0,limit=0)
	jobid<-unique(queryout$global_job_category_id)
	jobid<-as.integer(jobid)
	if(length(jobid)==0){
		data<-list(requisition_id=ReqId,global_job_category_id="NA",Region=data_center,Skills="No Skills")
		data<-toJSON(data)
		output <- dbInsertDocument(mongo, idcc, data)
		return("No Requisition Data")
	}
	jobjson<-list(global_job_category_id=jobid)
	jobjson<-toJSON(jobjson)
	keys<-list(requisition_id=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, req, jobjson, keys, skip=0,limit=0)
	reqidlist<-unique(as.integer(queryout$requisition_id))
	CandidateList <- icc_candilist(ReqId,mongo)
	if(CandidateList=="No Relevant Profile Available"){
		for(i in 1:length(reqidlist)){
			data<-list(requisition_id=reqidlist[i],global_job_category_id=jobid,Region=data_center,Skills="No Skills")
			data<-toJSON(data)
			output <- dbInsertDocument(mongo, idcc, data)
		}
		return("No Relevant Profile Available; Unable to create Ideal Characteristics Table")
	}
	if(length(CandidateList)==1){
		candjson<-list(candidate_id=CandidateList)
		candjson<-toJSON(candjson)
	}
	if(length(CandidateList)!=1){
		for(i in 1:length(CandidateList)){
			ll<-list(candidate_id=CandidateList[i])
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
	if(length(queryout)==0){
		for(i in 1:length(reqidlist)){
		
			reqjson<-list(requisition_id=reqidlist[i])
			reqjson<-toJSON(reqjson)
			keys<-list("requisition_id"=1)
			keys<-append(keys,list("_id"=0))
			keys<-toJSON(keys)
			queryout<-dbGetQueryForKeys(mongo, idcc, reqjson, keys, skip=0,limit=0)
			queryout<-queryout[!is.na(queryout)]
			if(length(queryout)!=0){
				out<-dbRemoveQuery(mongo, idcc, reqjson)
			}
			data<-list(requisition_id=reqidlist[i],global_job_category_id=jobid,Region=data_center,Skills="NA")
			data<-toJSON(data)
			output <- dbInsertDocument(mongo, idcc, data)
		}
	return("Done")
	}
	
	queryout<-queryout[,c(1,2)]
	Cand<-unique(queryout[,"candidate_id"])
	CandSkill<-data.frame()
	k<-0
	for(c in 1:length(Cand)){
		Temp <- queryout[queryout$candidate_id==Cand[c],]
		Temp<-Temp$parsedWords
		if(length(Temp)==0) next
		Temp<-fromJSON(Temp)
		if(length(Temp)==0) next
		k<-k+1
		temp<-rbindlist(Temp)
		temp$candidate_id<-Cand[c]
		if(k==0){CandSkill<-temp}
		if(k!=0){
			l<-list(CandSkill,temp)
			CandSkill<- rbindlist(l)
		}
	}
	CandSkill<-CandSkill$word
	if(length(CandSkill)==0){
		for(i in 1:length(reqidlist)){
		data<-list(requisition_id=reqidlist[i],global_job_category_id=jobid,Region=data_center,Skills="No Skills")
		data<-toJSON(data)
		output <- dbInsertDocument(mongo, idcc, data)
		}
		return("No Relevant Profile Available; Unable to create Ideal Characteristics Table")
	}
	CandSkill<-CandSkill[!is.na(CandSkill)]
	T<- as.data.frame(table(CandSkill))
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
	for(i in 1:length(reqidlist)){
		
		reqjson<-list(requisition_id=reqidlist[i])
		reqjson<-toJSON(reqjson)
		keys<-list("requisition"=1)
		keys<-append(keys,list("_id"=0))
		keys<-toJSON(keys)
		queryout<-dbGetQueryForKeys(mongo, idcc, reqjson, keys, skip=0,limit=0)
		queryout<-queryout[!is.na(queryout)]
		if(length(queryout)!=0){
			out<-dbRemoveQuery(mongo, idcc, reqjson)
		}
		data<-list(requisition_id=reqidlist[i],global_job_category_id=jobid,Region=data_center,Skills=Skills)
		data<-toJSON(data)
		output <- dbInsertDocument(mongo, idcc, data)
	}
	return("Success")
}

