library(rmongodb)
library(plyr)
library(reshape2)
source("rparam.r")
source("scorebatch.r")
	mongo <- mongo.create(host=host, db= db,username=username,password=password)
	coll<-"category_candidate_map"
	Catmap <- paste(db,coll,sep=".")
	coll<-"requisition"
	req <- paste(db,coll,sep=".")
	a<-mongo.distinct(mongo,Catmap,"global_job_category_id")
	for(m in 1:length(a)){
	print(paste("Global Job Id - ",a[m]))
	buf <- mongo.bson.buffer.create()
	T <- mongo.bson.buffer.append(buf,"global_job_category_id",a[m])
	query <- mongo.bson.from.buffer(buf)
	cursor <- mongo.find(mongo,Catmap, query,,list(candidates=1L,"_id"=0))
	CandidatesList <- mongo.cursor.to.data.frame(cursor)
	CandidatesList <- as.integer(CandidatesList[,1])
	cursor <- mongo.find(mongo,req, query,,list(requisition_id=1L,"_id"=0))
	RequisitionList <- mongo.cursor.to.data.frame(cursor)
	RequisitionList <- as.integer(RequisitionList[,1])
	##Collect Skills of CandidatesList
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
	for(i in 1:length(CandidatesList)){
		mongo.bson.buffer.start.object(buf, toString(i-1))
		# "a":1
		mongo.bson.buffer.append.int(buf, "candidateID", as.integer(CandidatesList[i]))
		# ... }
		mongo.bson.buffer.finish.object(buf)
	}
	mongo.bson.buffer.finish.object(buf)
	CandQuery <- mongo.bson.from.buffer(buf)
	cursor <- mongo.find.all(mongo, ins, CandQuery,,list(candidateID=1L,parsedWords.word=1L, parsedWords.interpreter_value=1L,"_id"=0))
	#cursor<- mongo.cursor.to.data.frame(cursor)
	CandAllSkill<-data.frame()
	CandSkill<-data.frame()
	tempallskill <-data.frame()
	tempskill<-data.frame()
	#print(Sys.time())
	test<-integer()
	for(i in 1:length(cursor)){
		if(i>1){
			CandAllSkill<- rbind.fill(CandAllSkill[colnames(CandAllSkill)], tempallskill[colnames(tempallskill)])
			CandSkill<- rbind.fill(CandSkill[colnames(CandSkill)], tempskill[colnames(tempskill)])
		}
		temp<-unlist(cursor[i])
		if(length(temp)<=1) next
		res<-ldply(cursor[i],data.frame)
		if(length(res)<3) next
		if(length(res)==3){
			test<-1
			tempallskill <- melt(res,id="candidateID","parsedWords.word",value.name='SkillSet')
			tempallskill <- tempallskill[,c(1,3)]
			tempskill <- melt(res,id="candidateID","parsedWords.word",value.name='SkillSet')
			tempskill <- tempskill[,c(1,3)]
			next
		}
		if(nrow(res)!=0){
			test<-0
			T1<-ncol(res)
			T1<-T1-1
			T1<-T1/2
			T1<-T1-1
			if(T1==0){
				test<-1
				tempallskill <- melt(res,id="candidateID","parsedWords.word",value.name='SkillSet')
				tempallskill <- tempallskill[,c(1,3)]
				tempskill <- melt(res,id="candidateID","parsedWords.word",value.name='SkillSet')
				tempskill <- tempskill[,c(1,3)]
				next
			}
			if(T1>3){
				test<-1
				query1<-character()
				query2<-character()
				for(j in 1:T1){
					c<-paste("parsedWords.word",j,sep=".")
					query1[j]<-c
					c<-paste("parsedWords.interpreter_value",j,sep=".")
					query2[j]<-c
				}
				T1<-"parsedWords.word"
				query1<-c(T1,query1)
				T1<-"parsedWords.interpreter_value"
				query2<-c(T1,query2)
				query<-c(query1,query2)
				tempallskill <- melt(res,id="candidateID",query1,value.name='SkillSet')
				tempallskill <- tempallskill[complete.cases(tempallskill),]
				tempallskill <- tempallskill[,c(1,3)]
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
					tempskill<-res2[,c(1,3)]
				}
				if(ll==0){
					tempskill<-data.frame(res$candidateID)
					tempskill$SkillSet<-'NA'
					colnames(tempskill)<-c("candidateID","SkillSet")
				}
			}
		}

	}

	if(test==1|length(cursor)==1){
		CandAllSkill<- rbind.fill(CandAllSkill[colnames(CandAllSkill)], tempallskill[colnames(tempallskill)])
		CandSkill<- rbind.fill(CandSkill[colnames(CandSkill)], tempskill[colnames(tempskill)])
	}
	if(length(cursor)==0){
		score<-data.frame(CandidatesList)
		score$RScore<-0
		score$PScore<-0
		score$EScore<-0
		for(j in 1:length(RequisitionList)){
	print(paste("Requisition -",RequisitionList[j]))
			ss_insert_zindex(RequisitionList[j],score,mongo)
			print("Scores Inserted")
		}
	}
	#print(paste("Finished Getting Parsed Data -",Sys.time()))
	if(length(CandAllSkill)==0){
		score<-data.frame(CandidatesList)
		score$RScore<-0
		score$PScore<-0
		score$EScore<-0
		for(j in 1:length(RequisitionList)){
			ss_insert_zindex(RequisitionList[j],score,mongo)
			print("Scores Inserted")
		}
		
	}
	Candd<-unique(CandAllSkill[,"candidateID"])
	CandNoResume<- setdiff(CandidatesList,Candd)
	if(length(CandNoResume)!=0){
		CandNoResume<-data.frame(CandNoResume)
		colnames(CandNoResume)<- 'candidateID'
		CandNoResume$SkillSet<-"NA"
		CandSkill<-rbind.fill(CandSkill[colnames(CandSkill)], CandNoResume[colnames(CandNoResume)])
		CandAllSkill<-rbind.fill(CandAllSkill[colnames(CandAllSkill)], CandNoResume[colnames(CandNoResume)])
	}
	###Getting data from Candidate collection for skills###
	coll <- "candidate"
	ins <- paste(db,coll,sep=".")
	candnoskill<-integer()
	k<-0
	res<-data.frame()
	for(i in 1:length(CandidatesList)){
		buf <- mongo.bson.buffer.create()
		T <- mongo.bson.buffer.append(buf,"candidate_id",CandidatesList[i])
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
			temp[[j]][1]<-NULL
		}
		temp<-ldply (temp, data.frame)
		temp<-melt(temp,id="candidate_id",value.name='SkillSet')
		temp<-temp[,c(1,3)]
		res <- rbind.fill(res[colnames(res)], temp[colnames(temp)])
	}
	if(nrow(res)!=0){
		colnames(res)<-c("candidateID","SkillSet")
		CandAllSkill<-rbind(res,CandAllSkill)
		CandSkill<-rbind(res,CandSkill)
	}
	##Score for every candidate
	for(j in 1:length(RequisitionList)){
		print(paste("Requisition - ",RequisitionList[j]))
		Status<-scorebatch(RequisitionList[j],mongo,CandidatesList,CandSkill,CandAllSkill)
		print(Status)
	}	
	}

