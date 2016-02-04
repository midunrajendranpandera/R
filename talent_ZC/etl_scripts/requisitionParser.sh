#! /bin/sh
#Command to start requisition parser incremental job on Linux
nohup python3 requisitionParser.py >> requisitionParser.log >2&1 &

