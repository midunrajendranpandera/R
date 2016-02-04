import json

def obj_dict(self):
   return self.__dict__

class ClassifierObject():
	def __init__(self, category_id, candidate_ary):
		self.global_job_category_id = category_id
		self.candidates = candidate_ary

	def toJson(self):
		return json.dumps(self, default=obj_dict)