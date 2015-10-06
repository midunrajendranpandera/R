
###############################################################################################
#   An R function to insert the zendex score of candidates for a Zero Chaos requisitions      #
#   These R functions are Copyright (C) of Pandera Systems LLP                                #
###############################################################################################

insert_zindex<- function(ReqId,Score,mongo)
{
	db <- "candidate_model"
	coll<-"requisition_cand_zendex_scores"
	ons <- paste(db,coll,sep=".")
	length<-nrow(Score)
	for(i in 1:length){
		temp<-Score[i,]
		buf <- mongo.bson.buffer.create()
		T <- mongo.bson.buffer.append(buf,"requisition_id",ReqId)
		T <- mongo.bson.buffer.append(buf,"candidate_id",temp$Cand)
		query <- mongo.bson.from.buffer(buf)
		count <- mongo.count(mongo, ons, query)
		if(count>=1) {T<-mongo.remove(mongo,ons,query)}
		Zindex<-temp$RScore+temp$EScore+temp$PScore
		#zendex_distribution=list(Experience=temp$EScore,Skills=temp$RScore,'Job Fit'=temp$PScore)
		zindex1=list(Name="Experience",Score=temp$EScore)
		zindex2=list(Name="Skills",Score=temp$RScore)
		zindex3<-list(Name="Job Fit",Score=temp$PScore)
		Out <- mongo.bson.from.list(list(candidate_id=as.integer(temp$Cand),requisition_id=as.integer(ReqId),zindex_score=Zindex,zindex_distribution=list(zindex1,zindex2,zindex3)))
		Insert <- mongo.insert(mongo,ons,Out)
	}
}
