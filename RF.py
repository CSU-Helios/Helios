import pymongo
import pprint
import urllib.request
import json
import time
import sys
import argparse
from ast import literal_eval as make_tuple
import numpy as np
import Helios
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier

import random

class RF:

    def __init__(self):
        self.helios = Helios.Helios()
        self._get_db_data()
        self.severity_rf = RandomForestClassifier()

    def query_bing(self, location):
        self.helios.loadMapData(False, None, location)

    def train_severity(self):
        Xtr, ytr, Xt, yt = self.get_data_for_severity()
        # print(Xt[0], yt[0])
        self.severity_rf.fit(Xtr, ytr)

    def test_severity(self):
        Xtr, ytr, Xt, yt = self.get_data_for_severity()
        # print(Xt[0], yt[0])
        return self.severity_rf.score(Xt, yt)

    def predict_severity(self, data):
        return self.severity_rf.predict(data)

    def get_data_for_severity(self):
        X = []
        y = []
        for data in self.data:
            ix = [data[i] for i in data if i == 'type' or i == 'roadClosed' or i == 'start' or i =='end']
            iy = [data[i] for i in data if i == 'severity']
            X.append(ix)
            y.append(iy)
        # print(X[0], '\n', y[0])
        return X[:int(self.data_size * .8)], y[:int(self.data_size * .8)], \
            X[int(self.data_size * .8):], y[int(self.data_size * .8):]

    def _get_db_data(self):
        self.data = []
        cursor = self.helios.col.find()
        for entry in cursor:
            element = {
                'start': int(entry['start'][6:-2]),
                'end' : int(entry['end'][6:-2]),
                'type' : entry['type'],
                'severity' : entry['severity'],
                'roadClosed' : entry['roadClosed'],
                'geoHash' : entry['point']['geohash']
            }
            self.data.append(element)
        self.data_size = len(self.data)



    def loc_in_geohash(self, geohash, gold):
        if str(geohash).startswith(gold):
            return True





if __name__ == "__main__":
    p = RF()

    p.train_severity()
    Xtr, ytr, Xt, yt = p.get_data_for_severity()
    print(type(yt))
    print(Xt[0], len(Xt))
    print(p.predict_severity(Xt))
    print(yt)
    print(p.test_severity())


    # one_loc = []
    # counter = 0
    # for x in p.X:
    #     counter += 1
    #     if p.loc_in_geohash(x[4], 'dp9'):
    #         one_loc.append(x)
    #
    # print(counter)
    # print(one_loc)
    # print(len(one_loc))


    # regr = RandomForestRegressor()
    # regr.fit(p.X, p.y)
    # X1 = np.array(X1).reshape(-1, 1)
    # print(len(p.X1))
    # for data, ans in zip(p.X1, p.y1):
    #     print(data)
    #     print(int(regr.predict([data])))
    #     print(ans)



