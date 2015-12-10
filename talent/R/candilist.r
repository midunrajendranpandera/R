###############################################################################################
#   An R function to retreive relevant candidates for a requisition                           #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################
library(rjson)

candilist <- function(ReqId,mongo)
{
	reqcan<-"requisition_candidate"
	reqjson<-list(requisition_id=ReqId)
	reqjson<-toJSON(reqjson)
	keys<-list(candidate_id=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, reqcan, reqjson, keys, skip=0,limit=0)
	queryout<-queryout[!is.na(queryout)]
	if(length(queryout)==0){
		return("No Candidates")
	}
	return(queryout)
}

