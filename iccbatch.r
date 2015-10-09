library(rmongodb)
library(plyr)
options(warn=-1)
source("/home/pandera/RCode/icc.r")
host <- 'devmongo01.zcdev.local:27000'
db <- "candidate_model"
username<-"candidateuser"
password<-"bdrH94b9tanQ"
mongo <- mongo.create(host=host, db= db,username=username,password=password)
coll<-"requisition_candidate"
ins <- paste(db,coll,sep=".")
a<-mongo.distinct(mongo,ins,"requisition_id")
l<-length(a)
for(m in 1598:l){
print(m)
print(a[m])
icc(a[m])
}
return("Done")
