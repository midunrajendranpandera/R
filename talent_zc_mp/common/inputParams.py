import json
from datetime import datetime, date, time

def obj_dict(self):
   return self.__dict__

class InputParamsObj():
   def __init__(self, reqId, isSubmitted, masterSupplierId, minScore):
      self.req_Id = reqId
      self.is_submitted = isSubmitted
      self.master_supplier_id = masterSupplierId
      self.min_score = minScore
      self.request_initiated = datetime.now().isoformat()
   def toJson(self):
      return json.dumps(self, default=obj_dict)

