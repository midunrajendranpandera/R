import json

def obj_dict(self):
		return self.__dict__

class ZcSummaryObj():
	def __init__(self, id, success, msg):
		self._id = id
		self.success = success
		self.message = msg

	def toJson(self):
		return json.dumps(self, default=obj_dict)