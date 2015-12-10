##Watchdog for restarting process on process termination

while [ 1 ]
do
pgrep -f resumeParser.py > candInc.temp
cat candInc.temp |wc -l > candInc.temp1
read t < candInc.temp1
if [ $t -eq 1 ]
then
echo "Process Running"
sleep 2m
elif [ $t -eq 0 ]
then
echo "Start the Process"
DATE=`date +%Y-%m-%d:%H:%M:%S`
nohup python3 -u resumeParser.py  > ./logs/candidate_incremental_$DATE.log &
sleep 1m
else
echo "Check the process"
sleep 2m
fi
done

