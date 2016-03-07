###############################################################################################
#   An R function to insert the zendex score of candidates for a Zero Chaos requisitions      #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################

ss_insert_zindex<- function(ReqId,Score,mongo)
{
	outputcoll<-"searchscore_cand_zindex_scores"
	length<-nrow(Score)
	for(i in 1:length){
		temp<-Score[i,]
		query<-list(requisition_id=ReqId)
		query<-toJSON(query)
		qtemp<-list(candidate_id=temp$Cand)		
		qtemp<-toJSON(qtemp)
		query<-c(query,qtemp)
		query<-toString(query)
		query<-paste("{$and:[",query)
		query<-paste(query,"]}")
		keys<-list(candidate_id=1)
		keys<-append(keys,list("_id"=0))
		keys<-toJSON(keys)
		queryout<-dbGetQueryForKeys(mongo, outputcoll, query, keys, skip=0,limit=0)
		queryout<-queryout[!is.na(queryout)]
		 if(length(queryout)!=0){
			out<-dbRemoveQuery(mongo, outputcoll, query)
		}	
		Zindex<-temp$RScore+temp$EScore+temp$PScore
		#zendex_distribution=list(Experience=temp$EScore,Skills=temp$RScore,'Job Fit'=temp$PScore)
		zindex1=list(name="Experience",score=temp$EScore)
		zindex2=list(name="Skills",score=temp$RScore)
		zindex3<-list(name="Job Fit",score=temp$PScore)
		data<-list(candidate_id=as.integer(temp$Cand),requisition_id=as.integer(ReqId),zindex_score=Zindex,zindex_distribution=list(zindex1,zindex2,zindex3))
		data<-toJSON(data)
		output <- dbInsertDocument(mongo, outputcoll, data)
	}
}
