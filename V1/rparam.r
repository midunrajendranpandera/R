###############################################################################################
#   An R Configuration File connectivity to Mongodb Zero Chaos requisitions                   #
###############################################################################################


###########################################################READ ME##############################################################
##																															   #
## This file has to be updated during deployment																		       #
## Comment unused variable using # (db, username and password)																   #
## Update 'hosts' variable for connecting to mongodb replica sets															   #
## Set replset to TRUE in the environment where there is a need to connect to mongodb replicasets. Also update hosts variable# #
##																															   #
################################################################################################################################


host <- 'devmongo01.zcdev.local'
port<-27000
#db <- "candidate_model"
#username<-"candidateuser"
#password<-"bdrH94b9tanQ"
hosts <-"10.17.90.213:27000 , 10.17.90.214:27000 , 10.17.90.215:27000"
db <- "candidate_model_qasaturn"
username<-"victor_pandera"
password<-"8Cv4v73D"

replset=TRUE
