library(rmongodb)
library(plyr)
options(warn=-1)
source("/home/pandera/RCode/zindex_main.r")
host <- 'devmongo01.zcdev.local:27000'
db <- "candidate_model"
username<-"candidateuser"
password<-"bdrH94b9tanQ"
mongo <- mongo.create(host=host, db= db,username=username,password=password)
coll<-"requisition_candidate"
ins <- paste(db,coll,sep=".")
a<-mongo.distinct(mongo,ins,"requisition_id")
total<-length(a)
for(m in 36:total){
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
                Status<-zindex_main(a[m],,candidateid)
                print(Status)
        }

}
return("Done")
