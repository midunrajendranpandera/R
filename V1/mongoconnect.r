###############################################################################################
#      R function to retreive relevant candidates for a requisition                           #
###############################################################################################

library(rJava)
source('rparam.r')
source('rm.r')

mongoconnect<-function(){
	if(replset){
		mongo<-mongoDbReplicaSetConnectWithCredentials(db,hosts,username,password)
		return(mongo)
	}
	if(!replset){
		mongo<-mongoDbConnect(db,host,port)
		authenticate<-dbAuthenticate(mongo,username,password)
		return(mongo)
	}


}
