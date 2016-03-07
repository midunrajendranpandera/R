import sys
import linecache
from ZCLogger import ZCLogger

logger = ZCLogger()

class DebugException():
	def __init__(self, e):
		print("Error %s" % e)
		exc_type, exc_obj, tb = sys.exc_info()
		f = tb.tb_frame
		lineno = tb.tb_lineno
		filename = f.f_code.co_filename
		linecache.checkcache(filename)
		line = linecache.getline(filename, lineno, f.f_globals)
		print ("EXCEPTION IN (%s, LINE %s \"%s\"): %s" % (filename, lineno, line.strip(), exc_obj))
		msg =  ("EXCEPTION IN (%s, LINE %s \"%s\"): %s" % (filename, lineno, line.strip(), exc_obj))
		logger.log(msg)
