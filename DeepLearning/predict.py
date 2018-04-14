import pymongo, datetime, copy, pickle, os, re, random
from py_geohash_any import geohash as gh
from multiprocessing import Pool
import numpy as np
from tensorflow import keras


_CLIENT = pymongo.MongoClient('localhost', 12345)
_DB = _CLIENT.Helios_Test
_COLLECTION = _DB.Helios_Traffic_Data
_PREDICTIONS = _DB.Helios_Traffic_Predictions
_HOUR = datetime.timedelta(seconds=60*60)
_DAY = datetime.timedelta(days=1)
_WEEK = datetime.timedelta(weeks=1)
_NOW = datetime.datetime.utcnow() - 6 * _HOUR
_PRECISION = 7


def timeToMillis(time):
	"""
	Converts python datetime to milliseconds
	"""
	epoch = datetime.datetime.utcfromtimestamp(0)
	return int((time - epoch).total_seconds() * 1e3)
	
def millisToTime(millis):
	"""
	Converts milliseconds to python datetime
	"""
	return datetime.datetime.utcfromtimestamp(millis/1e3)
	
def query(geo, start_time, end_time):
	"""
	Queries the database for the number of incidents occuring between
	start_time and end_time in the current geohash
	"""
	records = list(_PREDICTIONS.find({'geohash': {'$regex': '^'+geo}}))
	if start_time < _NOW:
		records.extend(_COLLECTION.find({'point.geohash': {'$regex': '^'+geo}, "type": 2}))
		records.extend(_COLLECTION.find({'toPoint.geohash': {'$regex': '^'+geo}, "type": 2}))
	count = 0
	start_time = timeToMillis(start_time)
	end_time = timeToMillis(end_time)
	for rec in records:
		if int(re.search('\((.*)\)', rec['start']).group(1)) <= end_time and int(re.search('\((.*)\)', rec['end']).group(1)) >= start_time:
			if "incidents" in rec:
				count += int(rec["incidents"])
			else:
				count += 1
	return np.asarray(count)
	
def getInput(geo, end_time):
	"""
	Creates the input for the given target
	"""
	times = [(end_time-_HOUR,end_time), (end_time-_DAY,end_time), (end_time-_DAY,end_time-_DAY+_HOUR), (end_time-2*_HOUR,end_time-_HOUR)] #Times that we would like to query geohash for
	inp = np.zeros((9*len(times)))
	ix = 0
	nbrs = gh.neighbors(geo)
	for start_time, end_time in times:
		inp[ix] = query(geo, start_time, end_time)
		for i, d in enumerate(['n','ne','e','se','s','sw','w','nw']): #Gets all neighbors of current geohash
			ix += 1
			inp[ix] = query(nbrs[d], start_time, end_time)
	return inp
	
def makeInput(target, dt):
	"""
	Helper to append metadata to current input
	"""
	date = dt.date()
	time = dt.time()
	weekday = np.zeros((7))
	weekday[date.weekday()] = 1.0
	hour_time = np.zeros((24))
	hour_time[time.hour] = 1.0
	return np.hstack((target,weekday,hour_time))
	
def getGeohashes(start_time, end_time):
	start_time = timeToMillis(start_time)
	end_time = timeToMillis(end_time)
	entries = list(_COLLECTION.find())
	geohashes = set()
	for entry in entries:
		#if int(re.search('\((.*)\)', entry['start']).group(1)) <= end_time and int(re.search('\((.*)\)', entry['end']).group(1)) >= start_time:
		geohashes.add(entry["point"]["geohash"][:_PRECISION])
		geohashes.add(entry["toPoint"]["geohash"][:_PRECISION])
	return geohashes

def insertAtTime(geo, value, time):
	print("here")
	record = {"geohash": geo,"incidents": str(value), "start": "/Date(" + str(timeToMillis(time)) + ")/", 
	          "end": "/Date(" + str(timeToMillis(time+_HOUR)) + ")/"}
	_PREDICTIONS.insert_one(record)
	
def predictFromData(model, time):
	geohashes = getGeohashes(time-_WEEK, time-_WEEK+_HOUR)
	for geo in geohashes:
		inp = makeInput(getInput(geo[:_PRECISION], time), time)
		prediction = np.argmax(model.predict(inp.reshape(1,-1)))
		if prediction > 0:
			insertAtTime(geo, prediction, time)
	
def predict(model):
	time = copy.deepcopy(_NOW)
	for i in range(24):
		print("\rOn hour {} of 24".format(i+1), end="")
		predictFromData(model, time)
		time += _HOUR
	
if __name__=="__main__":
	_PREDICTIONS.delete_many({})
	model = keras.models.load_model('/home/kevin/Helios/DeepLearning/saved_model.h5')
	predict(model)
