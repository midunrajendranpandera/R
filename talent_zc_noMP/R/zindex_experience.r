###############################################################################################
#   An R function to calculate the candidates probabilistics score for Zero Chaos requisitions#
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################


zindex_experience <- function(ReqId,mongo,candskill,reqskill,Candidate)
{
	##Getting skills data from Requisition parsed collection##################
#print("Inside Exp")
	candskill<-data.frame(candskill)
	if(nrow(candskill)==0){
		Scores<-data.frame(Candidate)
		colnames(Scores)<- 'Cand'
		Scores$EScore<-0
		return(Scores)
	}
	reqskill<-trimws(reqskill$word,which="both")
	Cand<-unique(candskill[,"candidate_id"])
	levels<-length(Cand)
	Scores<-data.frame(Cand)
	EScore<-integer()
#print("Comparing using soundex")
		for(i in 1:levels){
			Temp <- candskill[candskill$candidate_id==Cand[i],]
			Temp<-Temp[complete.cases(Temp),]
			T <- Temp[,2]
			T<- as.data.frame(table(T))
			T<-as.character(T[,1])
			len<-length(T)
			if(len==0) {
				EScore[i]<-0
				next
			}
			Value<-0
			for(j in 1:length(reqskill)){
				Check<- stringdist(reqskill[j],T,method='soundex')
				if(sum(Check)<len){
					Value<-Value+1
				}
			}
			if(Value==0){
				EScore[i]<-0
			}
			else{
				EScore[i] <- ceiling((Value*40)/length(reqskill))
			}
		}
		Scores$EScore <- EScore
		missedcand<-as.integer(setdiff(Candidate,Cand))

		if(length(missedcand)!=0){
			missedcand<-data.frame(missedcand)
			colnames(missedcand)<- 'Cand'
			missedcand$EScore<-0
			Scores<-rbind.fill(Scores[colnames(Scores)], missedcand[colnames(missedcand)])
		}
		return(Scores)
	
}

