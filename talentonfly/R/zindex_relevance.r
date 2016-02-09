###############################################################################################
#   An R function to calculate the candidates relevance score for Zero Chaos requisitions     #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################


zindex_relevance <- function(ReqId,mongo,CandallSkill,reqallskill)
{
print("Inside Relevance")
	Cand<-unique(CandallSkill[,"candidate_id"])
	Scores<-data.frame(Cand)
	levels<-max(rank(Cand))
	RScore<-integer()
	totalskills<-0.7*nrow(reqallskill)
	for(i in 1:levels){
		Temp <- CandallSkill[CandallSkill$candidate_id==Cand[i],]
		T <- Temp[,2]
		T<- as.data.frame(table(T))
		T <- arrange(T,desc(Freq))
		count <- NROW(T)
		Check<-reqallskill$'word' %in% T$T
		intersection<-sum(Check)
		RScore[i] <- ceiling(40*(intersection/totalskills))
		if(RScore[i]>40)RScore[i]=40
	}
	Scores$RScore <- RScore
	
	return(Scores)
}

