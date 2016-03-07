###############################################################################################
#   An R function to calculate the candidates probabilistics score for Zero Chaos requisitions
#   These R functions are Copyright (C) of Pandera Systems LLP
###############################################################################################

zindex_probabilistics <- function(ReqId,mongo,CandallSkill)
{
	print("Inside PScore")
	idcc <- "ideal_candidate_characteritics"
	
	
	reqjson<-list(requisition_id=as.integer(ReqId))
	reqjson<-toJSON(reqjson)
	
	keys<-list("Skills"=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, idcc, reqjson, keys, skip=0,limit=0)
	test<-queryout[!is.na(queryout)]
	
	if(length(test)==0){
		return("No Ideal Table")
	}
	if(test=="No Skills"){
		return("No Ideal Table")
	}
	idcskill<-queryout$Skills
	idcskill<-fromJSON(idcskill)
	
	idcskill <- as.data.frame(idcskill)
	colnames(idcskill)<-"Skill"
	idealskilllength<-0.7*nrow(idcskill)
	Cand<-unique(CandallSkill[,"candidate_id"])
	Scores<-data.frame(Cand)
	levels<-max(rank(Cand))
	PScore<-integer()
	for(i in 1:levels){
		Temp <- CandallSkill[CandallSkill$candidate_id==Cand[i],]
		T <- Temp[,2]
		T<- as.data.frame(table(T))
		T <- arrange(T,desc(Freq))
		count <- NROW(T)
		Check<-idcskill$'Skill' %in% T$T
		PScore[i] <- ceiling((20*sum(Check)/idealskilllength))
		if(PScore[i] >20) PScore[i]<-20
	}
	Scores$PScore <- PScore
	return(Scores)
}

