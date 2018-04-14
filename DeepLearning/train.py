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
	"""
	Helper to get get all times between start and end date
	at increments of _TIME_DIFF
	"""
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
	
def getData():
	"""
	Function to create all data with inputs and targets
	"""
	names = ["point","toPoint"]
	inp = []
	out = []
	collection = list(_COLLECTION.find())
	size = len(collection) #Select all records in database
	geohashes = set([d["point"]["geohash"] for d in collection])
	for i, data in enumerate(collection):
		if data["type"] != 2:
			continue
		print("\rCreating data progress {:.5}%".format((i+1)/(size+1)*100),end="\t\t")
		start_date = millisToTime(int(re.search('\((.*)\)', data['start']).group(1)))
		end_date = millisToTime(int(re.search('\((.*)\)', data['end']).group(1)))
		datetimes = getDatetimes(start_date, end_date)
		for name in names:
			for dt in datetimes:
				for _ in range(24):
					target = query(data[name]['geohash'][:_PRECISION],dt,_HOUR+dt)
					out.append(target)
					inp.append(makeInput(getInput(data[name]['geohash'][:_PRECISION],dt), dt))
					
					extra_geo = random.sample(geohashes,1)[0]
					target = query(extra_geo[:_PRECISION],dt,_HOUR+dt) #used to add zeros to training set
					out.append(target)
					inp.append(makeInput(getInput(extra_geo[:_PRECISION],dt), dt))
					
					dt += _HOUR
	print()
	return np.asarray(inp), np.asarray(out)

def getModel():
	"""
	Creates model
	"""
	inp = keras.layers.Input(shape=(67,))
	model = keras.layers.Dense(64, use_bias=True)(inp)
	model = keras.layers.Dropout(0.5)(model)
	#model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Activation("relu")(model)
	model = keras.layers.Dense(64, use_bias=True)(model)
	model = keras.layers.Dropout(0.5)(model)
	#model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Activation("relu")(model)
	model = keras.layers.Dense(64, use_bias=True)(model)
	model = keras.layers.Dropout(0.5)(model)
	#model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Activation("relu")(model)
	model = keras.layers.Dense(64, use_bias=True)(model)
	model = keras.layers.Dropout(0.5)(model)
	#model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Activation("relu")(model)
	model = keras.layers.Dense(7, activation="softmax", use_bias=True)(model)
	model = keras.models.Model(inp, model)
	model.compile(optimizer=keras.optimizers.Adam(), loss='categorical_crossentropy', metrics=['acc'])
	return model
	
if __name__=="__main__":
	if not os.path.isfile("data.pkl") or _NEW_DATA:
		with open("data.pkl", "wb") as f:
			data = getData()
			pickle.dump(data, f)
	else:
		with open("data.pkl", "rb") as f:
			 data = pickle.load(f)
	unique, counts = np.unique(data[1], return_counts=True)
	weights = dict(zip(unique, len(data[1])/counts))
	print(weights)
	targets = np.zeros((data[1].size,7))
	targets[np.arange(data[1].size), data[1]] = 1
	model = getModel()
	callbacks = [keras.callbacks.ModelCheckpoint("saved_model.h5", monitor='val_loss', verbose=1, save_best_only=True)]
	model.fit(data[0], targets, batch_size=32, epochs=20, verbose=1, callbacks=callbacks, validation_split=0.2)#, class_weight=weights)
