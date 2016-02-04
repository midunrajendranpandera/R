#! /bin/sh


#Now, start the ZC search web service
#Execute the Python-Bottle script in background

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
 
cd - 