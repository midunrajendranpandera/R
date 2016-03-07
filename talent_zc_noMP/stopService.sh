#! /bin/sh

#-- Kill the watchdog script
kill -9 `pgrep -f searchServiceWatchdog.sh`

#-- Kill currently running processes
kill -9 `pgrep -f zcSearchService.py`

