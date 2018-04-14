import pymongo, datetime, copy, pickle, os, re, random
from py_geohash_any import geohash as gh
from multiprocessing import Pool
import numpy as np
from tensorflow import keras

_PRECISION = 7
_START_DATE = datetime.datetime(2018, 3, 26, hour=10, minute=0)
_END_DATE = datetime.datetime(2018, 3, 28, hour=20, minute=0)
_TIME_DIFF = datetime.timedelta(seconds=60*60)
_HOUR = datetime.timedelta(seconds=60*60)
_DAY = datetime.timedelta(days=1)
_INPUT_SIZE = (32,32)
_NEW_DATA = False
_PROCESSES = 7
_CLIENT = pymongo.MongoClient('localhost', 12345)
_DB = _CLIENT.Helios_Test
_COLLECTION = _DB.Helios_Traffic_Data

def getDatetimes(start_date, end_date):
	datetimes = []
	start = copy.deepcopy(start_date)
	while start <= end_date:
		datetimes.append(start)
		start += _TIME_DIFF
	return datetimes
	
def timeToMillis(time):
	"""
	Converts python datetime to milliseconds
	"""
	epoch = datetime.datetime.utcfromtimestamp(0)
	return (time - epoch).total_seconds() * 1e3
	
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
	start_time = timeToMillis(start_time)
	end_time = timeToMillis(end_time)
	records =  list(_COLLECTION.find({'point.geohash': {'$regex': '^'+geo}, "type": 2}))
	records.extend(_COLLECTION.find({'toPoint.geohash': {'$regex': '^'+geo}, "type": 2}))
	count = 0
	for rec in records:
		if int(re.search('\((.*)\)', rec['start']).group(1)) <= end_time and int(re.search('\((.*)\)', rec['end']).group(1)) >= start_time:
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
	date = dt.date()
	time = dt.time()
	weekday = np.zeros((7))
	weekday[date.weekday()] = 1.0
	hour_time = np.zeros((24))
	hour_time[time.hour] = 1.0
	return np.hstack((target,weekday,hour_time))
	
def getData(num=1):
	names = ["point","toPoint"]
	inp = []
	out = []
	entries = list(_COLLECTION.find())
	geohashes = set([d["point"]["geohash"] for d in entries])
	size = len(entries)
	random.shuffle(entries)
	for i, data in enumerate(entries):
		if data["type"] != 2:
			continue
		start_date = millisToTime(int(re.search('\((.*)\)', data['start']).group(1)))
		end_date = millisToTime(int(re.search('\((.*)\)', data['end']).group(1)))
		datetimes = getDatetimes(start_date, end_date)
		if np.random.rand() > 0.8:
			datetimes.append(datetimes[-1]+datetime.timedelta(hours=np.random.randint(9)))
		for dt in datetimes:
			for name in names:
				target = query(data[name]['geohash'][:_PRECISION],dt,_HOUR+dt)
				out.append(target)
				inp.append(makeInput(getInput(data[name]['geohash'][:_PRECISION],dt), dt))
				
				extra_geo = random.sample(geohashes,1)[0]
				target = query(extra_geo[:_PRECISION],dt,_HOUR+dt) #used to add zeros to training set
				out.append(target)
				inp.append(makeInput(getInput(extra_geo[:_PRECISION],dt), dt))
				if len(out) >= num:
					return np.asarray(inp), np.asarray(out)
	print()
	return np.asarray(inp), np.asarray(out)

if __name__=="__main__":
	data = getData()
	model = keras.models.load_model('/home/kevin/Helios/DeepLearning/saved_model.h5')
	pred = model.predict(data[0])
	print(pred)
	print(data, np.argmax(pred[0]), np.argmax(pred[1]))
	#print(data, model.predict(data[0]))
