from datetime import datetime, timedelta
import numpy as np
import json
import pymongo
from sklearn.ensemble import RandomForestClassifier


class Helios_RFC:

    def __init__(self):
        self.host = "localhost"
        self.port = 27017
        self.pred_database_name = "Helios_Test"
        self.pred_collection_name = "RF_prediction"
        self.helios_collection_name = "Helios_Traffic_Data"
        self._precision = 7
        self._debug = False

        self.pred_col, self.helios_col = self.setup_mongo()
        self.data = self._get_db_data()
        self.rfc = RandomForestClassifier()
        self.locations = self._get_loc_set()
        self.X_train, self.y_train, self.X_test, self.y_test = self._get_train_test_data()

    def train(self):
        """
        trains sklearn's implementation of a Ramdom Forest Classifier on data generated by
        _get_train_array()
        """
        self.log("train")
        self.rfc.fit(self.X_train, np.ravel(self.y_train))

    def test(self):
        self.log("test")
        return self.rfc.score(self.X_test, self.y_test)

    def predict_all_one_day_into_the_future(self):
        """
        returns an array that contains arrays that each have a geohash, timestamp and
        predicted number of incidents for all hours of the day one day into the future
        """
        self.log("predict_all_at_specified_time")

        predictions = []
        today = datetime.utcnow().date()
        prediction_time = datetime(today.year, today.month, today.day + 1)
        for loc_num in self.locations:
            for hour in range(1, 25):
                start_date = "/Date(" + str(int(self.time_to_millis(prediction_time))) + ")/"
                end_date = "/Date(" + str(int(self.time_to_millis(prediction_time
                                                                  + timedelta(seconds=60*60)))) + ")/"
                incidents = int(self._predict(loc_num, hour))
                if incidents:
                    prediction = {
                        'geohash': self.locations[loc_num],
                        'start': start_date,
                        'end': end_date,
                        'incidents': incidents,
                    }
                    predictions.append(prediction)
        return predictions

    def send_predictions_to_mongo(self, predictions):
        self.log("send_predictions_to mongo" + "First prediction:" + str(predictions[0]))
        self.pred_col.drop()
        for prediction in predictions:
            self.pred_col.insert_one(json.loads(json.dumps(prediction)))

    def setup_mongo(self):
        client = pymongo.MongoClient(self.host, self.port)
        db = client[self.pred_database_name]
        return db[self.pred_collection_name], db[self.helios_collection_name]
    
    def _get_loc_set(self):
        """
        private method that creates a set containing all unique geohashes in database at a
        certain precision level
        """
        self.log("get_loc_set")

        locations = {}
        for i, data in enumerate(self.data):
            locations[i] = data['geoHash'][:self._precision]
        return locations

    def _get_data_by_hour(self, geoHash):
        """
        private method that At every location makes an array that represents hours in a
        day with the number of incidents that occurred within that hour
        """
        self.log("get_data_by_hour")

        d = [0 for _ in range(24)]
        for item in self.data:
            if item['geoHash'] == geoHash:
                d[item['start'].hour & 24] += 1
        return d

    def _get_train_test_data(self):
        """
        private method that creates arrays used to train RFC using data from self.data,
        additional data can be added in the future
        """
        self.log("get_train_array")

        X_train = []
        y_train = []
        X_test = []
        y_test = []
        for i, loc_num in enumerate(self.locations):
            for hour, incidents in enumerate(self._get_data_by_hour(self.locations[loc_num])):
                if i % 5 == 0:
                    X_test.append([loc_num, hour])
                    y_test.append([incidents])
                else:
                    X_train.append([loc_num, hour])
                    y_train.append([incidents])
        return X_train, y_train, X_test, y_test

    def _get_db_data(self):
        """
        private method that loads all data from mongo db into self.data not all parameters
        loaded are used
        """
        self.log("_get_db_data")

        data = []
        cursor = self.helios_col.find()
        for entry in cursor:
            element = {
                'start': self.millis_to_time(int(entry['start'][6:-2])),
                # 'end': self.millis_to_time(int(entry['end'][6:-2])),
                # 'type': entry['type'],
                # 'severity': entry['severity'],
                # 'roadClosed': entry['roadClosed'],
                'geoHash': entry['point']['geohash'][:7]
            }
            data.append(element)
        return data

    def _predict(self, geohash_num, hour):
        """
        private method that takes a number which is a key for a specific geohash and the hour of the
         day for the prediction and returns the predicted number of incidences
        """
        self.log("_predict()")

        return self.rfc.predict([[geohash_num, hour]])[0]

    def millis_to_time(self, millis):
        """
        Converts milliseconds to python datetime
        """
        self.log("millis_to_time")
    
        return datetime.fromtimestamp(millis / 1000.0)
    
    def time_to_millis(self, time):
        """
        Converts python datetime to milliseconds
        """
        self.log("time_to_millis")
        
        epoch = datetime.utcfromtimestamp(0)
        return (time - epoch).total_seconds() * 1000.0
    
    def log(self, s):
        if self._debug:
            print(s)


if __name__ == "__main__":
    H_rfc = Helios_RFC()
    H_rfc.train()
    pred_data = H_rfc.predict_all_one_day_into_the_future()
    H_rfc.send_predictions_to_mongo(pred_data)
    score = H_rfc.test()
    print("Score:", score)
    

