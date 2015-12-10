#! /bin/bash

##Watchdog for restarting zcSearchService

alias trim="sed -e 's/^[[:space:]]*//g' -e 's/[[:space:]]*\$//g'"

function jsonValue() {
KEY=$1
num=$2
awk -F"[,:}]" '{for(i=1;i<=NF;i++){if($i~/'$KEY'\042/){print $(i+1)}}}' | tr -d '"' | sed -n ${num}p
}



### Function to get the url based on the environment from ConfigFile.properties
getUrl(){

   while IFS= read -r line;do
      #echo "$line"
      fields=($(printf "%s" "$line"|cut -d'=' --output-delimiter=' ' -f1-))
      #echo "${fields[0]}"

      if [ "${fields[0]}" = "url.server_location" ]; then
         export server=${fields[1]}
      fi 

      if [ "${fields[0]}" = "url.server_port" ]; then
         export port=${fields[1]}
      fi 

      if [ "${fields[0]}" = "url.timeout" ]; then
         export timeout=${fields[1]}
      fi 
   done < ./common/ConfigFile.properties

   #echo "Server is : [$server]"
   #echo "Port is   : [$port]"
   export url="http://$server:$port/isServiceAlive/"
   #echo "url is    : [$url]"
   #echo "timeout is : [$timeout]"

   return $URL
}

### <TODO> Make this read from Config file
#$url="http://QAPYR01.zcdev.local:80/isServiceAlive/"

getUrl
echo "url is           : [$url]"
echo "timeout limit is : [$timeout] sec"

while [ 1 ]
do
rm -f service.tmp
#read url < test
#curl -s -m 300 -X GET  $url | /usr/local/bin/python3 -c 'import json,sys;obj=json.load(sys.stdin);print(obj["success"])' > service.tmp
curl -s -m $timeout -X GET $url | jsonValue success 1 | trim > service.tmp
if [ -s service.tmp ]
then
#cat service.tmp |head -1 | cut -c10-13 > service.status
read status < service.tmp
if [[ $status =  "True" || $status = "true" ]]
then
DATE=`date "+%Y-%m-%d %H:%M:%S"`
echo "[$DATE] Service Running Fine"
sleep 1m
elif [[ $status = "False" || $status = "false" ]]
then
kill -9 `pgrep -f zcSearchService.py`
nohup /usr/local/bin/python3 zcSearchService.py > searchService.log &
DATE=`date "+%Y-%m-%d %H:%M:%S"`
echo "[$DATE] Service returned False with DB connectivity. Restart the service "
sleep 1m
fi
else
kill -9 `pgrep -f zcSearchService.py`
nohup /usr/local/bin/python3 zcSearchService.py > searchService.log &
DATE=`date "+%Y-%m-%d %H:%M:%S"`
echo "[$DATE] Service returned no response. Restart the service "
sleep 1m
fi
done
