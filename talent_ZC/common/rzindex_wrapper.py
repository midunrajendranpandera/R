import pyRserve
import configparser
from debugException import DebugException

config = configparser.ConfigParser()
config.read('../common/ConfigFile.properties')

r_conn_string = config.get("RSection", "r.main_location")
r_conn_requisition = config.get("RSection", "r.requisition")
r_conn_candidate = config.get("RSection", "r.candidate")
#print(r_conn_candidate)

def rzindex_wrapper(ReqId, cand):
	zindex={}
	try:
		conn=pyRserve.connect()
		conn.voidEval(r_conn_string)
		score=conn.r.zindex_main(ReqId,'c',cand)
		conn.close()
		j=len(score[0])
		for num in range(0,j):
			candidate_id=score[0][num]
			zindex[candidate_id] = {
				"zindex_distribution" : [
					{
						"name" : "Experience",
						"score" : score[3][num]
					},
					{
						"name" : "Skills",
						"score" : score[1][num]
					},
					{
						"name" : "Job Fit",
						"score" : score[2][num]
					}
				],
				"zindex_score" : score[5][num]
			}
	except Exception as e:
		DebugException(e)
		conn.close();

	return zindex


def rzindex_candidate(Candlist):
	try:
		conn=pyRserve.connect()
		conn.voidEval(r_conn_candidate)
		status=conn.r.candidate_incremental(Candlist)
		conn.close()
	except Exception as e:
		print("HERE - %s" % e)
		DebugException(e)
		conn.close();

def rzindex_wrapper_insert(ReqId, cand):
	try:
		conn=pyRserve.connect()
		conn.voidEval(r_conn_string)
		status=conn.r.zindex_main(ReqId,'c',cand)
		conn.close()
		print("Success")
	except Exception as e:
		DebugException(e)
		conn.close();


def rzindex_wrapper_newreq(ReqId):
	try:
		conn=pyRserve.connect()
		conn.voidEval(r_conn_requisition)
		status=conn.r.reqScoring(ReqId)
		conn.close()
		print("Success")
	except Exception as e:
		DebugException(e)
		conn.close();
