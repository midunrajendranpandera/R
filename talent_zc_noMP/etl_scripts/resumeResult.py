class ResumeResult():
	def __init__(self, resumeID, candidateID, datacenter):
		self.resume_id = resumeID
		self.candidate_id = candidateID
		self.data_center = datacenter
		self.parsedWords = []