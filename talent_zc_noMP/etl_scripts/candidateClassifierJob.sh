#! /bin/sh
#Command to start candidate classifier incremental job on Linux
nohup python3 candidateClassifierJob.py >> candidateClassifierJob.log >2&1 &

