#! /usr/local/bin/python3.4
import sys
sys.path.append('./controllers')
import bottle
import pymongo
from bottle import route, run, request, response
import zcCandidateScore, zcSearchAndScore
#from zcCandidateScore import getCandidateScore
#from zcSearchAndScore import getSearchAndScore
from debugException import DebugException
from ZCLogger import ZCLogger
from datetime import datetime, date, time

#uri = "mongodb://candidateuser:bdrH94b9tanQ@devmongo01.zcdev.local:27000/?authSource=candidate_model"
#client=MongoClient(uri)
#db=client.candidate_model
logger = ZCLogger()

is_local_dev = True

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

    print("[searchAndScore]  reqId [" + reqId + "] minScore [" + minScore + "]")

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
        return ("Internal Error: %s" % e)

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
        return("Internal Error: %s" % e)

if is_local_dev:
    run(host='localhost', port=8080, debug=True)
else:
    run(host='devpyr01.zcdev.local', port=8080, debug=True)

