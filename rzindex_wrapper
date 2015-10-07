import pyRserve

def rzindex_wrapper(reqId, cand):
	conn=pyRserve.connect()
	conn.voidEval('source("/home/pandera/RCode/zindex_main.r",chdir=T)')
	score=conn.r.zindex_main(reqid,'r',cand)
	j=len(score[0])
	zindex=[]
	for num in range(0,j):
		zend=[]
		dist={}
		dist1={}
		dist["candidate_id"]=score[0][num]
		dist["requisition_id"]=score[4][num]
		dist["zindex_score"]=score[5][num]
		dist1["name"]="Experience"
		dist1["score"]=score[3][num]
		zend.append(dist1)
		dist1={}
		dist1["name"]="Skills"
		dist1["score"]=score[1][num]
		zend.append(dist1)
		dist1={}
		dist1["name"]="Job Fit"
		dist1["score"]=score[2][num]
		zend.append(dist1)
		dist["zindex_distribution"]=zend
		zindex.append(dist)
	conn.close()
	return(zindex)