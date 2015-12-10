##Watchdog for restarting process on process termination

while [ 1 ]
do
pgrep -f requisitionIncremental.py > reqInc.temp
cat reqInc.temp |wc -l > reqInc.temp1
read t < reqInc.temp1
if [ $t -eq 1 ]
then
echo "Process Running"
sleep 2m
elif [ $t -eq 0 ]
then
echo "Start the Process"
DATE=`date +%Y-%m-%d:%H:%M:%S`
nohup python3 -u requisitionIncremental.py  > ./logs/requisition_incremental_$DATE.log &
sleep 1m
else
echo "Check the process"
sleep 2m
fi
done

