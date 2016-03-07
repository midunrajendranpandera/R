	library(rJava)
	library(rjson)
	source('rm.r')
	library(RMongo)
	library(data.table)
	library(plyr)
	source("mongoconnect.r")
	source("zindex_main.r")
	source("ss_zindex_main.r")
	source("rparam.r")
	source("icc.r")
	library(stringdist)
	options(warn=-1)
	options( java.parameters = "-Xmx5g" )

reqScoring<-function(requisitionid){
	mongo<-mongoconnect()
	req<-"requisition"
	Catmap<-"category_candidate_map"
	sscc<-"searchscore_cand_zindex_scores"
	requisitionid<-unlist(requisitionid) 	
	subCandList<- candilist(requisitionid,mongo)
	if(subCandList!="No Candidates"){
		status<-zindex_main(requisitionid,'c',subCandList)
	}
	#print(paste("Working on Requisition - ",requisitionid))
	reqjson<-list(requisition_id=requisitionid)
	Status<-icc(requisitionid)
	reqjson<-toJSON(reqjson)
	keys<-list("global_job_category_id"=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, req, reqjson, keys, skip=0,limit=0)
	jobid<-queryout$global_job_category_id
	jobid<-unique(jobid)
	reqjson<-list(global_job_category_id=as.integer(jobid))
	reqjson<-toJSON(reqjson)
	keys<-list("candidates"=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, Catmap, reqjson, keys, skip=0,limit=0)
	if(nrow(queryout)==0){
		#print("No Candidates")
		return()
	}
	CandidatesList<-queryout$candidates
	CandidatesList<-as.integer(fromJSON(CandidatesList))
	CandidatesList<-unique(CandidatesList)
	idcc <- "ideal_candidate_characteritics"
	reqjson<-list(requisition_id=as.integer(requisitionid))
	reqjson<-toJSON(reqjson)
	keys<-list("Skills"=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, idcc, reqjson, keys, skip=0,limit=0)
	test<-queryout[!is.na(queryout)]
	if(length(test)==0 | test=="No Skills"){
		idcskill<-"No Ideal Table"
	}
	if(test!="No Skills"){
		idcskill<-queryout$Skills
		idcskill<-fromJSON(idcskill)

		idcskill <- as.data.frame(idcskill)
		colnames(idcskill)<-"Skill"
	}
	CandList<-split(CandidatesList, ceiling(seq_along(CandidatesList)/200))
	Candlen<-length(CandList)
	#print(paste("Total Candidate Count(X200) - ",Candlen))
	dbDisconnect(mongo)				
	for(j in 1:Candlen){
		mongo<-mongoconnect()
		## Getting data from parsed resume##
		#print(paste("Getting Parsed Data -",Sys.time()))
		#print(paste("Candidate Batch - ",j))
		CandidatesList<-as.integer(unlist(CandList[j]))
		#print("Getting Parsed Skills")
		canparse <- "candidate_skills_from_parsed_resumes"
		if(length(CandidatesList)==1){
			candjson<-list(candidate_id=CandidatesList)
			candjson<-toJSON(candjson)
		}
		if(length(CandidatesList)!=1){
			for(i in 1:length(CandidatesList)){
				ll<-list(candidate_id=CandidatesList[i])
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
			score<-data.frame(CandidatesList)
			score$RScore<-0
			score$PScore<-0
			score$EScore<-0
			ss_insert_zindex(ReqId,score,mongo)
			dbDisconnect(mongo)
			#print("Scores Inserted")
			next
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
			score<-data.frame(CandidatesList)
			score$RScore<-0
			score$PScore<-0
			score$EScore<-0
			ss_insert_zindex(ReqId,score,mongo)
			#print("Scores Inserted")
			dbDisconnect(mongo)
			next
		}
		CandSkill<-CandallSkill[CandallSkill$interpreter_value=="skills"]
		CandallSkill<-data.frame(CandallSkill)

		CandallSkill<-subset(CandallSkill, select=c("candidate_id","word"))
		CandSkill<-subset(CandSkill, select=c("candidate_id","word"))

		Candd<-unique(CandallSkill[,"candidate_id"])
		CandNoResume<-setdiff(CandidatesList,Candd)

		if(length(CandNoResume)!=0){
			CandNoResume<-data.frame(CandNoResume)
			colnames(CandNoResume)<- 'candidate_id'
			CandNoResume$word<-"NA"
			CandallSkill<-rbind.fill(CandallSkill[colnames(CandallSkill)], CandNoResume[colnames(CandNoResume)])
		}
		dbDisconnect(mongo)
		###Getting data from Candidate collection for skills###
		mongo<-mongoconnect()
		#print("Getting Skills from Candidate Collection")
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
		dbDisconnect(mongo)
		#print(paste("Requisition id - ",requisitionid))
		Status<-ss_zindex_main(requisitionid,'c',CandidatesList,CandallSkill,CandSkill,idcskill)
		#print(Status)
	}
	return("Corresponding candidates scored")
}

