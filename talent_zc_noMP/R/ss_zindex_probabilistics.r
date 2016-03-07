###############################################################################################
#    R function to calculate the candidates probabilistics score for Zero Chaos requisitions  #
###############################################################################################

ss_zindex_probabilistics <- function(ReqId,mongo,CandallSkill,idcskill)
{
	if(idcskill=="No Ideal Table"){
		return(idcskill)
	}
	#print("Inside PScore")
	Cand<-unique(CandallSkill[,"candidate_id"])
	Scores<-data.frame(Cand)
	levels<-max(rank(Cand))
	PScore<-integer()
	idealskilllength<-0.7*nrow(idcskill)
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

