#! /bin/sh

export DEST_PATH=$1
export ENVIRONMENT=$2
export ETL_LOGS=$DEST_PATH/etl_scripts/logs

#-- Exit if the DEST_PATH env variable is not set
if [ -z "${DEST_PATH}" ]; then
   echo ""
   echo "ERROR: Deployment destination path [DEST_PATH] not supplied"
   echo "Exiting deployment"
   echo ""
   exit 10
fi

#Check if the destination folder exists
if [ ! -d "${DEST_PATH}" ]; then
   echo ""
   echo "The deployment destination folder doesn't exist. Exiting deployment"
   echo ""
   exit 20
fi

if [ -z "${ENVIRONMENT}" ]; then
   echo ""
   echo "The environment was not provided. Exiting deployment"
   echo ""
   exit 30
fi


#-- Kill incremental etl watchdogs
kill -9 `pgrep -f subCandidateProcess.sh`
kill -9 `pgrep -f reqIncremental.sh`
kill -9 `pgrep -f candidateIncremental.sh`

#-- Kill the watchdog script
kill -9 `pgrep -f searchServiceWatchdog.sh`

#-- Kill currently running processes
killall Rserve
killall python3

#-- Now clear then copy the files into the DEST_PATH folder
#-- excluding the config files that will come later

rsync -av --exclude-from=deploy.exclude * $DEST_PATH/ --delete

#-- Copy appropriate config files to the destination
cp common/ConfigFile.properties.$ENVIRONMENT $DEST_PATH/common/ConfigFile.properties
cp R/rparam.r.$ENVIRONMENT $DEST_PATH/R/rparam.r


#Start R service first
#Should have been set PATH to the R executable in the environment
R CMD Rserve
if [ $? -eq 0 ]; then
   echo "R service started successfully!"
else
   echo "The R service not started and exit status is : $?"
   exit 11
fi

#Now, start the ZC search web service
#Execute the Python-Bottle script in background

cd $DEST_PATH
nohup python3 zcSearchService.py > searchService.log &
#cd -
sleep 2
pgrep -f zcSearchService.py
if [ $? -eq 0 ]; then
   echo "Web service started successfully!"
else
   echo "The web service is not started and exit status is : $?"
   exit 12
fi

#-- Start the watchdog script
nohup sh searchServiceWatchdog.sh >> watchdog.log &
 
pgrep -f searchServiceWatchdog.sh
if [ $? -eq 0 ]; then
   echo "watchdog script started successfully"
else
   echo "watchdog script has not started : $?"
   exit 12
fi
 
#-- Start incremental etl jobs

cd $DEST_PATH/etl_scripts
 
nohup sh subCandidateProcess.sh > $ETL_LOGS/subCandidateProcess.log &
 
pgrep -f subCandidateProcess.sh
if [ $? -eq 0 ]; then
   echo "Submitted Candidate Incremental Job Started Successfully"
else
   echo "Submitted Candidate Incremental has not started : $?"
   exit 12
fi
 
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
