###############################################################################################
#      R function to return  the zendex score of candidates for a Zero Chaos requisitions     #
###############################################################################################
return_zindex<- function(ReqId,Score)
{
	ScoresJ<-Score
	ScoresJ$requisition_id<-ReqId
	ScoresJ$zindexscore<-with(ScoresJ,RScore+PScore+EScore)
	colnames(ScoresJ)<-c("candidate_id","Skills","Job Fit","Experience","requisition_id","zindex_score")
	return(ScoresJ)
}

