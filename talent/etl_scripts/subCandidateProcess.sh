
##Watchdog for restarting process on process termination

while [ 1 ]
do
pgrep -f submitted_candidate_scoring.py > subcand.temp
cat subcand.temp |wc -l > subcand.temp1
read t < subcand.temp1
if [ $t -eq 1 ]
then
echo "Process Running"
sleep 2m
elif [ $t -gt 1 ]
then
echo "Check the process"
sleep 2m
else
echo "Start the Process"
DATE=`date +%Y-%m-%d:%H:%M:%S`
nohup python3 -u submitted_candidate_scoring.py  > ./logs/submitted_candidate_incremental_$DATE.log &
sleep 1m
fi
done

