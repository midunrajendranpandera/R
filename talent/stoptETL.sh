#! /bin/sh

#-- Kill incremental etl watchdogs
kill -9 `pgrep -f subCandidateProcess.sh`
kill -9 `pgrep -f reqIncremental.sh`
kill -9 `pgrep -f candidateIncremental.sh`

#-- Kill incremental Python Scripts
kill -9 `pgrep -f submitted_candidate_scoring.py`
kill -9 `pgrep -f requisitionIncremental.py`
kill -9 `pgrep -f resumeParser.py`

