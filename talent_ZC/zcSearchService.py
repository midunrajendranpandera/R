#! /usr/local/bin/python3.4
import sys
sys.path.append('./controllers')
sys.path.append('./common')
import bottle
import pymongo
import configparser
from bottle import route, run, request, response
import zcCandidateScore, zcSearchAndScore
from debugException import DebugException
from ZCLogger import ZCLogger
from datetime import datetime, date, time
from zcSummaryObj import ZcSummaryObj
#import cherrypy
import checkService

config = configparser.ConfigParser()
config.read('./common/ConfigFile.properties')

host = config.get("URLSection", "url.server_location")
port = config.get("URLSection", "url.server_port")
debug = config.get("URLSection", "url.debug")

logger = ZCLogger()

def enable_cors(fn):
    def _enable_cors(*args, **kwargs):
        # set CORS headers
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

        if bottle.request.method != 'OPTIONS':
            # actual request; reply with the actual response
            return fn(*args, **kwargs)

    return _enable_cors

@enable_cors
@route('/searchAndScore/',  method=['OPTIONS','GET'])
def searchAndScore():

    callStartTime = datetime.now().isoformat()

    reqId = request.GET.get('requisition_id', '').strip()
    minScore = request.GET.get('min_score', '').strip()

    #print("[searchAndScore]  reqId [" + reqId + "] minScore [" + minScore + "]")

    msg = ("[SearchAndScore]  CallStartTime [" + callStartTime + "]  reqId [" + reqId + "]  minScore [" + minScore +"]")
    logger.log(msg)

    if minScore is '':
        minScore = 0

    try:
        summary=zcSearchAndScore.getSearchAndScore(int(reqId), int(minScore))
        print(summary)
        return (summary)
    except Exception as e:
        DebugException(e)
        response.status = 500
        #return ("Internal Error: %s" % e)
        msg = ("Internal Error: %s" % e)
        summaryObj = ZcSummaryObj(-1, False, msg)
        return (summaryObj.toJson())

@enable_cors
@route('/candidateScore/',  method=['OPTIONS','GET'])
def candidateScore():
    #qryDict = parse_qs(request.query_string)
    callStartTime = datetime.now().isoformat()

    reqId = request.GET.get('requisition_id', '').strip()
    isSubmitted = request.GET.get('issubmitted', '').strip()
    masterSupplierId = request.GET.get('master_supplier_id', '').strip()
    minScore = request.GET.get('min_score', '').strip()
    
    print("[candidateScore]  reqId [" + reqId + "]  isSubmitted [" + isSubmitted + "]   masterSupplierId [" + masterSupplierId + "]  minScore [" + minScore +"]")
    msg = ("[candidateScore]  CallStartTime [" + callStartTime + "]  reqId [" + reqId + "]  isSubmitted [" + isSubmitted + "]   masterSupplierId [" + masterSupplierId + "]  minScore [" + minScore +"]")

    logger.log(msg)
    try:
        summary=zcCandidateScore.getCandidateScore(int(reqId), int(isSubmitted), int(masterSupplierId), int(minScore))
        return (summary)
    except Exception as e:
        DebugException(e)
        response.status = 500
        #return("Internal Error: %s" % e)
        msg = ("Internal Error: %s" % e)
        summaryObj = ZcSummaryObj(-1, False, msg)
        return (summaryObj.toJson())

# Check if service is live and responding
@enable_cors
@route('/isServiceAlive/',  method=['OPTIONS','GET'])
def isServiceAlive():
    callStartTime = datetime.now().isoformat()
    msg = "[isServiceAlive] Call start time [" + callStartTime + "]"
    logger.log(msg)
    successStatus = False
    try:
        resp=checkService.getServiceStatus()
        msg = "Service status returned [" + str(resp) + "]"
        if( resp == 1):
            successStatus = True
        else:
            successStatus = False
        summaryObj = ZcSummaryObj(resp, successStatus, msg)
        return (summaryObj.toJson())
    except Exception as e:
        DebugException(e)
        response.status = 500
        msg = ("Internal Error: %s" % e)
        return(msg) 
        #summaryObj = ZcSummaryObj(-1, False, msg)
        #return (summaryObj.toJson())


#bottle.run(host=host, port=port, debug=debug,server='cherrypy')
bottle.run(host=host, port=port, debug=debug)


