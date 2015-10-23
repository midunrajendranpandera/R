###############################################################################################
#      R function to calculate the candidates relevance score for Zero Chaos requisitions     #
###############################################################################################


zindex_relevance <- function(ReqId,mongo,CandallSkill,reqallskill)
{
#print("Inside Relevance")
	Cand<-unique(CandallSkill[,"candidate_id"])
	Scores<-data.frame(Cand)
	levels<-max(rank(Cand))
	RScore<-integer()
	for(i in 1:levels){
		Temp <- CandallSkill[CandallSkill$candidate_id==Cand[i],]
		T <- Temp[,2]
		T<- as.data.frame(table(T))
		T <- arrange(T,desc(Freq))
		count <- NROW(T)
		Check<-reqallskill$'word' %in% T$T
		div<-0.5*length(Check)
		RScore[i] <- ceiling(40*(sum(Check)/div))
		if(RScore[i]>40)RScore[i]=40
	}
	Scores$RScore <- RScore
	
	return(Scores)
}

