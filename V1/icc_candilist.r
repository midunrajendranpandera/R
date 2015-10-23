###############################################################################################
#            R function to retreive relevant candidates for a requisition                     #
###############################################################################################

source('candilist.r')

icc_candilist <- function(ReqId,mongo)
{
	reqcan<-"requisition_candidate"
	req<-"requisition"
	reqjson<-list(requisition_id=ReqId)
	reqjson<-toJSON(reqjson)
	keys<-list("global_job_category_id"=1)
	keys<-append(keys,list("requisition_id"=1))
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, req, reqjson, keys, skip=0,limit=0)
	jobid<-unique(queryout$global_job_category_id)
	jobjson<-list(global_job_category_id=jobid)
	jobjson<-toJSON(jobjson)
	keys<-list(requisition_id=1)
	keys<-append(keys,list("_id"=0))
	keys<-toJSON(keys)
	queryout<-dbGetQueryForKeys(mongo, req, jobjson, keys, skip=0,limit=0)
	reqid<-unique(as.integer(queryout$requisition_id))
	k<-0
	candidateid<-integer()
	for( i in 1:length(reqid)){
		
		temp<-list(requisition_id=reqid[i])
		temp<-toJSON(temp)
		query<-temp
		temp<-list(is_hired=1)
		temp<-toJSON(temp)
		query<-c(query,temp)
		query<-toString(query)
		query<-paste("{$and:[",query)
		query<-paste(query,"]}")
		keys<-list(candidate_id=1)
		keys<-append(keys,list("_id"=0))
		keys<-toJSON(keys)
		queryout<-dbGetQueryForKeys(mongo, reqcan, query, keys, skip=0,limit=0)
		cand<-unique(as.integer(queryout$candidate_id))
		cand<-cand[!is.na(cand)]
		if(length(cand)==0) next
		if(length(cand)==1)
		{
			k<-k+1
			candidateid[k]<-cand
			next
		}
		if(length(cand)!=0){
			for (j in 1:length(cand)){
				k<-k+1
				candidateid[k]<-cand[j]
			}
			
		}
	}
	candidateid<-unique(candidateid)
	candidateid<-candidateid[!is.na(candidateid)]
	if (length(candidateid)==0){
		candidateid <- candilist(ReqId,mongo)
	}
	if(candidateid=="No Candidates"){
		return("No Relevant Profile Available")
	}
	return(candidateid)
}

