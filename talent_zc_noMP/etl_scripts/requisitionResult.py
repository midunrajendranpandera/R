class RequisitionResult:
	def __init__(self, requisitionId, dataCenter):
		self.requisition_id = requisitionId
		self.data_center = dataCenter
		self.parsedWords = []