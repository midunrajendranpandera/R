library(rmongodb)
library(plyr)
options(warn=-1)
source("zindex_main.r")
source("rparam.r")
mongo <- mongo.create(host=host, db= db,username=username,password=password)
coll<-"requisition_candidate"
ins <- paste(db,coll,sep=".")
a<-mongo.distinct(mongo,ins,"requisition_id")
total<-length(a)
for(m in 1:total){
	print(m)
	print(a[m])
	buf <- mongo.bson.buffer.create()
	T <- mongo.bson.buffer.append(buf,"requisition_id",a[m])
	query <- mongo.bson.from.buffer(buf)
	cursor <- mongo.find(mongo, ins, query,,list(candidate_id=1L))
	candidateid<-mongo.cursor.to.data.frame(cursor)
	T<-nrow(candidateid)
	if(T==0){
		next
	}
	if(T!=0){
		candidateid<-as.integer(candidateid[,1])
		candidateid<-unique(candidateid)
		Status<-zindex_main(a[m],'c',candidateid)
		print(Status)
	}
}
print("Done")

