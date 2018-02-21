
import Helios

class PShell:

	"""
	PShell: Interactive Python Class to retrieve data from MongoDB
	
	Attributes:
		collection (Mongo Collection): a instance of Helios Mongo Collection
	    center (tuple): Zoom center coordinates, default as "denver"
	    zoomRange (int): Zoom range, default as 1 degree
	"""
	
	def __init__(self):
		"""
		Initiation
		"""
		self.collection = Helios().retrieveCol()

		# Zoom Range set default to 1 degree, approximately 85 km
		self.zoomRange = 1

		# Zoom center coordinates, default as "denver"
		self.center = (39, -105)

	def setZoomRange(self, zoomRange):
		"""
		set zoom range function
		Args:
		    zoomRange (int): a new zoom range
		"""
		self.zoomRange = zoomRange

	def setCenter(self, center):
		"""
		set zoom center, either takes in a coordinates tuple or city name
		set the class center variable to a new coordinates tuple
		Args:
		    center (tuple): a new zoom center coordinates as (latitude, longitude)
		    center (string): a new zoom center city name
		"""
		pass

	def queryFind(self):
		"""
		************************************************************
		pymongo find method
		************************************************************
		
		find(filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False, 
		cursor_type=CursorType.NON_TAILABLE, sort=None, allow_partial_results=False, 
		oplog_replay=False, modifiers=None, batch_size=0, manipulate=True, collation=None, 
		hint=None, max_scan=None, max_time_ms=None, max=None, min=None, return_key=False, 
		show_record_id=False, snapshot=False, comment=None, session=None)

		Documenation: http://api.mongodb.com/python/current/api/pymongo/collection.html

		Query the database.

		TODO:
			generate query based on demand
		"""
		pass

	def retrieveEntries(self, center = None, zoomRange = None, numlimit = 0):
		"""Summary
		
		Args:
		    center (None, optional): a center for retrieving entries
		    zoomRange (None, optional): a range around center to retrieve entries
		    numlimit (int, optional): a limit of total number of entries to send back
		
		TODO:
			This method should call queryFind(), give a number of parameters
			and return a list of entries

		Returns:
		    list: a list of entries
		"""
		return listOfEntires

