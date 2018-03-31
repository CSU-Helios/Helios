import numpy as np
from tensorflow import keras
import datetime, os, random, copy

_END_TIME = datetime.datetime(2018, 3, 26, hour=10, minute=0)
_FIFTEEN_MINUTES = datetime.timedelta(minutes=15)
_HOUR = datetime.timedelta(hours=1)
_BATCH_SIZE = 32
_INPUT_SHAPE = (32,32)
_INPUT_SIZE = (32,32,16)
_TARGET_SHAPE = (32,32,16)
_META_INPUT_SHAPE = (31,)


def makeTarget(target_temporal, geo):
	target = np.zeros(_TARGET_SHAPE)
	temporal = copy.deepcopy(target_temporal)
	ix = 0
	end = False
	for _ in range(16):
		aggr = np.zeros((32,32,4))
		ix2 = 0
		for _ in range(4):
			if temporal > _END_TIME:
				end = True
			temporal += _FIFTEEN_MINUTES
			try:
				aggr[:,:,ix2] = np.load("data/"+geo+"/"+str(temporal)+".npy")
			except FileNotFoundError:
				pass
			ix2 += 1
		target[:,:,ix] = np.sum(aggr,axis=2)
		ix += 1
	for _ in range(0):
		if temporal > _END_TIME:
			end = True
		temporal += _HOUR
		try:
			target[:,:,ix] = np.load("data/"+geo+"/"+str(temporal)+".npy")
		except FileNotFoundError:
			pass
		ix += 1
	if np.sum(target) == 0:
		end = True
	target *= 100
	return target, end
	

def makeData(geohashes, count_data=False):
	while True:
		count = 0
		random.shuffle(geohashes)
		hours = []
		minutes = []
		meta = []
		targets = []
		for geo in geohashes:
			for date in sorted(os.listdir("data/"+geo)):
				central_temporal = datetime.datetime.strptime(date[:-4], '%Y-%m-%d %H:%M:%S')
				t, end = makeTarget(central_temporal, geo)
				if end:
					break
				targets.append(t)
				count += 1
				if count_data:
					targets = []
					continue
					
				#Create hours input array
				x = []
				temporal = copy.deepcopy(central_temporal)
				inp = np.zeros((32,32,16))
				ix = 0
				for _ in range(16):
					aggr = np.zeros((32,32,4))
					ix2 = 0
					for _ in range(4):
						temporal -= _FIFTEEN_MINUTES
						try:
							aggr[:,:,ix2] = np.load("data/"+geo+"/"+str(temporal)+".npy")
						except FileNotFoundError:
							pass
						ix2 += 1
					inp[:,:,ix] = np.sum(aggr,axis=2)
					ix += 1
				
				hours.append(inp)
				#Create metadata for "central" date
				central_date = central_temporal.date()
				central_time = central_temporal.time()
				weekday = np.zeros((7))
				weekday[central_date.weekday()] = 1.0
				hour_time = np.zeros((24))
				hour_time[central_time.hour] = 1.0
				meta.append(np.hstack((weekday, hour_time)))
				
				#Create minutes input array
				x = []
				temporal = copy.deepcopy(central_temporal)
				for _ in range(16):
					try:
						x.append(np.load("data/"+geo+"/"+str(temporal)+".npy"))
					except FileNotFoundError:
						x.append(np.zeros((32,32)))
					temporal -= _FIFTEEN_MINUTES
				minutes.append(x)
				
				if len(minutes) == _BATCH_SIZE and len(hours) == _BATCH_SIZE and not count_data:
					yield [np.rollaxis(np.asarray(minutes),1,4), np.asarray(hours), \
					      np.asarray(meta)], np.asarray(targets)
					minutes = []
					hours = []
					meta = []
					targets = []
		if count_data:
			yield count
			

def getModel():
	minutes_input = keras.layers.Input(shape=_INPUT_SIZE)
	hours_input = keras.layers.Input(shape=_INPUT_SIZE)
	meta_input = keras.layers.Input(shape=_META_INPUT_SHAPE)
	output = []
	for input in [hours_input, minutes_input]:
		model = keras.layers.Conv2D(64, 3, strides=1, activation='elu', use_bias=False)(input)
		model = keras.layers.BatchNormalization()(model)
		model = keras.layers.Conv2D(64, 3, strides=2, use_bias=True)(model)
		model = keras.layers.Dropout(0.5)(model)
		model = keras.layers.BatchNormalization()(model)
		model = keras.layers.Activation('elu')(model)
		model = keras.layers.Conv2D(32, 3, strides=1, use_bias=True)(model)
		model = keras.layers.Dropout(0.5)(model)
		model = keras.layers.BatchNormalization()(model)
		model = keras.layers.Activation('elu')(model)
		model = keras.layers.Conv2D(32, 3, strides=2, use_bias=True)(model)
		model = keras.layers.Dropout(0.5)(model)
		model = keras.layers.BatchNormalization()(model)
		model = keras.layers.Activation('elu')(model)
		model = keras.layers.Conv2D(16, 3, strides=1, use_bias=True)(model)
		model = keras.layers.Dropout(0.5)(model)
		model = keras.layers.BatchNormalization()(model)
		model = keras.layers.Activation('elu')(model)
		model = keras.layers.Conv2D(16, 3, strides=2, use_bias=True)(model)
		model = keras.layers.Dropout(0.5)(model)
		model = keras.layers.BatchNormalization()(model)
		model = keras.layers.Activation('elu')(model)
		output.append(keras.layers.Flatten()(model))
	meta = keras.layers.Dense(32, activation='tanh', use_bias=True)(meta_input)
	model = keras.layers.Concatenate()([output[0], output[1], meta])
	model = keras.layers.Dropout(0.5)(model)
	model = keras.layers.Dense(64, activation='tanh', use_bias=True)(model)
	model = keras.layers.Reshape((8,8,1))(model)
	model = keras.layers.Dropout(0.5)(model)
	model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Conv2DTranspose(16, 3, strides=1, padding='same', use_bias=True)(model)
	model = keras.layers.Dropout(0.5)(model)
	model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Activation('elu')(model)
	model = keras.layers.Conv2DTranspose(16, 3, strides=2, padding='same', use_bias=True)(model)
	model = keras.layers.Dropout(0.5)(model)
	model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Activation('elu')(model)
	model = keras.layers.Conv2DTranspose(32, 3, strides=1, padding='same', use_bias=True)(model)
	model = keras.layers.Dropout(0.5)(model)
	model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Activation('elu')(model)
	model = keras.layers.Conv2DTranspose(32, 3, strides=2, padding='same', use_bias=True)(model)
	model = keras.layers.Dropout(0.5)(model)
	model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Activation('elu')(model)
	model = keras.layers.Conv2DTranspose(64, 3, strides=1, padding='same', use_bias=True)(model)
	model = keras.layers.Dropout(0.5)(model)
	model = keras.layers.BatchNormalization()(model)
	model = keras.layers.Activation('elu')(model)
	model = keras.layers.Conv2DTranspose(16, 3, strides=1, padding='same', activation='relu', use_bias=True)(model)
	inputs = [minutes_input, hours_input, meta_input]
	model = keras.models.Model(inputs=inputs, outputs=model)
	model.compile(optimizer=keras.optimizers.RMSprop(), loss='mse')
	return model
	
	
def train(training, validation, model, train_steps, test_steps):
	callbacks = [keras.callbacks.ModelCheckpoint("saved_model.h5", monitor='val_loss', verbose=1, save_best_only=True)]
	model.fit_generator(makeData(training), steps_per_epoch=train_steps, epochs=10, verbose=1, \
	                    validation_data=makeData(validation), validation_steps=test_steps, \
	                    callbacks=callbacks)
	

if __name__ == "__main__":
	geohashes = os.listdir("data")
	random.shuffle(geohashes)
	split = int(len(geohashes) * 0.8)
	training = geohashes[:split]
	testing = geohashes[split:]
	print("Training geohashes: {}\tTesting geohashes: {}".format(len(training), len(testing)))
	model = getModel()
	train(training, testing, model, makeData(training, count_data=True).__next__(), \
	      makeData(training, count_data=True).__next__())
