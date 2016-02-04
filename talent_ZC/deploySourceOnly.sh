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
echo "incremental etl watchdogs killed"

#-- Kill incremental processes
kill -9 `pgrep -f submitted_candidate_scoring.py`
kill -9 `pgrep -f requisitionIncremental.py`
kill -9 `pgrep -f resumeParser.py`
echo "incremental etl processes killed"

#-- Kill the watchdog script
kill -9 `pgrep -f searchServiceWatchdog.sh`
echo "service watchdog killed"


#-- Kill currently running processes
#killall Rserve
#killall python3
kill -9 `pgrep -f zcSearchService.py`
echo "service process killed"

#-- Now clear then copy the files into the DEST_PATH folder
#-- excluding the config files that will come later

rsync -av --exclude-from=deploy.exclude * $DEST_PATH/ --delete

#-- Copy appropriate config files to the destination
cp common/ConfigFile.properties.$ENVIRONMENT $DEST_PATH/common/ConfigFile.properties
#cp R/rparam.r.$ENVIRONMENT $DEST_PATH/R/rparam.r

echo "Source copy complete"

