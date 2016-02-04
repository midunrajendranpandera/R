import logging

class ZCLogger():
   def __init__(self):
      logging.basicConfig(filename="talentSearchService.log",
                          filemode='a',
                          format='%(asctime)s,%(msecs)d %(name)s %(levelname)-8s %(message)s',
                          #datefmt='%H:%M:%S',
                          datefmt='%a, %d %b %Y %H:%M:%S',
                          level=logging.DEBUG)

      logging.info("Running talent search service")
      self.logger = logging.getLogger('talentSearch')

   def log(self, msg):
      self.logger.info(msg)
