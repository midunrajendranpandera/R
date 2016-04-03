#! /bin/sh
 
#-- Start incremental etl jobs

cd ./etl_scripts
 
#nohup sh subCandidateProcess.sh > $ETL_LOGS/subCandidateProcess.log &
 
#pgrep -f subCandidateProcess.sh
#if [ $? -eq 0 ]; then
#   echo "Submitted Candidate Incremental Job Started Successfully"
#else
#   echo "Submitted Candidate Incremental has not started : $?"
#   exit 12
#fi
 
nohup sh reqIncremental.sh > $ETL_LOGS/reqIncremental.log &
 
pgrep -f reqIncremental.sh
if [ $? -eq 0 ]; then
   echo "Requisition Incremental Job Started Successfully"
else
   echo "Requisition Incremental has not started : $?"
   exit 12
fi
 
nohup sh candidateIncremental.sh > $ETL_LOGS/candidateIncremental.log &
 
pgrep -f candidateIncremental.sh
if [ $? -eq 0 ]; then
   echo "Candidate Incremental Job Started Successfully"
else
   echo "Candidate Incremental has not started : $?"
   exit 12
fi

cd -
