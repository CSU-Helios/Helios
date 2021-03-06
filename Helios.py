import pymongo
import pprint
import urllib.request
import json
import time
import sys
import argparse
from ast import literal_eval as make_tuple

class Helios:

    """
    Functions:
        1. Get a instance of existing collection of Helios
        2. Retrieve traffic data from Bing Map API
    """
    
    def _cons_Names(self):
        """
        All constant valuables assignments
        """
        self.host = "localhost"
        self.port = 27017
        self.dbName = "Helios"
        self.colName = "TrafficData"
        self.primaryKey = "incidentId"
        self.logfileName = "./log/Helios_Traffic_Data_Log-" + time.ctime().replace(" ", "-") + ".log"
        self.citiesFileName = "./resources/cities.json"
        self.__base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
        
        # Coordinates are more or less centered on Denver
        # (southLatitude, westLongitude, northLatitude, eastLongitude)
        self.loc = "denver"
        self.log = False
        
        
    def __init__(self, firstTimeSetUp=False, confirm=False, log=False, dbName=None, colName=None):
        """
        Initiation, including load constant names, connect to Bing Map API,
        connect to an openning MongoDB, connect to Mongod database, 
        connect to Mongod collection
        If firstTimeSetUp, it will clean the existing database, named self.dbName
        
        Args:
            firstTimeSetUp (bool, optional): Set up Helios MongoDB for the first time
            confirm (bool, optional): Double Confirm when firstTimeSetUp
            log (bool, optional): store log file into ./log
            dbName (None, optional): specify database name to be connected
            colName (None, optional): specify collection name to be connected
        """

        self._cons_Names()

        self._connectMongoDB(self.host, self.port)
        
        if firstTimeSetUp:
            if confirm:
                self._cleanUpDatabase(self.dbName, confirm)
            else: print("ERROR: Not cleanning up database, please specific 'confirm'")
            
            
        if dbName is None: self._connectDatabase(self.dbName)
        else: self._connectDatabase(dbName)
        if colName is None: self._connectCollection(self.colName)
        else: self._connectCollection(colName)

        if firstTimeSetUp:
            self._mongoColConf(self.primaryKey)

        if log:
            self.log = True
            self.logfile = open(self.logfileName, "w")

    def __del__(self):
        if self.log:
            self.logfile.close()
            
    def _loadBingAPI(self, location):
        """
        Using Bing Map Key and Query to build connection with Bing Map API
        
        Args:
            location (Tuple): Location specific where BING API retrieve traffic data from
        """
        # Bing Maps dev key
        self.key = "AnJj6c_AqKZdGB2Zk8IYb7AjivBKxMppvGEaXgf3RdQdqES8MOqHxjz1W_HhniD3"
        # Bing Maps Query url with para
        self.url = "http://dev.virtualearth.net/REST/V1/Traffic/Incidents/" \
                    + str(location[0]) + ',' \
                    + str(location[1]) + ',' \
                    + str(location[2]) + ',' \
                    + str(location[3]) \
                    + "/true?s=1,2,3,4&t=1,2,3,4,5,6,7,8,9,10,11&key=" \
                    + self.key
        
    def _connectMongoDB(self, host, port):
        """
        Connect to MongoD and create a client instance
        
        Args:
            host (String): the host Mongod used
            port (Int): the port Mongod listened
        """
        self.client = pymongo.MongoClient(host, port)
        
    def _connectDatabase(self, datebase):
        """
        Connect to MongoD client and open a database instance
        
        Args:
            datebase (String): The database name for Helios
        """
        self.db = self.client[datebase]
        print("Connected to Database", datebase)
    
    def _connectCollection(self, collection):
        """
        Connect to MongoD database and open a collection instance
        
        Args:
            collection (String): The collection name for Helios
        """
        self.col = self.db[collection]
        print("Connected to Collection", collection)
        
    def _cleanUpDatabase(self, databaseName=None, confirm=False):
        """
        !DANGEROUS
        Cleaning up / Removing all the database instances in Mongod
        PRIVATE METHOD
        
        Args:
            databaseName (String, optional): The database name for Helios
            confirm (bool, optional): Double Confirm
        """
        if confirm:
            if not databaseName:
                for name in self.client.database_names():
                    self.client.drop_database(name)
            else:
                self.client.drop_database(databaseName)
        print("Cleaned up Database", databaseName)
                
    def _cleanUpCollection(self, collectionName=None, confirm=False):
        """
        !DANGEROUS
        Cleaning up / Removing all the collection instances in Mongod
        PRIVATE METHOD
        
        Args:
            collectionName (String, optional): The collection name for Helios
            confirm (bool, optional): Description
        
        Deleted Parameters:
            Confirm (bool, optional): Double Confirm
        """
        if confirm:
            if not collectionName:
                for name in self.client.collection_names():
                    self.db.drop_collection(name)
            else:
                self.db.drop_collection(collectionName)
        print("Cleaned up Database", collectionName)
    

    def _mongoColConf(self, primaryKey):
        """
        CAUTION: This Method should be called only ONCE
        Construct the initial configuration of Helios Data Collection
        More configuration can be added through MongoShell or Pymongo
        
        Args:
            primaryKey (String): The primary key for Helios Traffic Collection
        """
        # TODO
        # https://api.mongodb.com/python/current/api/pymongo/index.html
            
        # create primary key "incidentId"
        self.col.create_index([(primaryKey, pymongo.ASCENDING)], unique=True)
    
    def _queryBingMap(self, url=None):
        """
        Query Bing Map API and retrieve data
        [https://msdn.microsoft.com/en-us/library/hh441726.aspx]
        
        Args:
            url (String, optional): Query URL
        
        Returns:
            TYPE: list
            https://msdn.microsoft.com/en-us/library/hh441730.aspx
        """
        if not url:
            url = self.url
            
        request = urllib.request.Request(url)
        response = urllib.request.urlopen(request)
        text = response.read().decode(encoding="utf-8")
        return json.loads(text)
    
    def _safeInsert(self, json):
        """
        Insert One JSON formated record if there is no primary key confliction
        
        Args:
            json (json): one json formated record
        
        Returns:
            bool: True for success, False for fail
        """
        priKeyVal = json[self.primaryKey]
        if self.col.find({self.primaryKey: priKeyVal}).count() > 0:
            return False
        else:
            self.col.insert_one(json).inserted_id
            return True
        
    def _getQueryBox(self, location):
        """
        Generate a query box around a location by 6 degrees (500km maximum)
        
        Args:
            location (String, optional): pull location coordinates from existing file
        
        Returns:
            locationBox (LeftLat, BotLong, RightLat, TopLong)
        """
        longitude, latitude = 0, 0
        # Temporarily remove support for tuple, cause it raise error
        # when parsing "New York" (treat it as tuple while it doesn't)
        # try:
        #     longitude, latitude = make_tuple(location)
        #     print(longitude, latitude)
        # except ValueError: 
        cities = json.load(open(self.citiesFileName))
        cityNames = [city["city"].lower() for city in cities]
        if location.lower() in cityNames:
            idx = cityNames.index(location.lower())
            longitude = int(cities[idx]["longitude"])
            latitude = int(cities[idx]["latitude"])
        else:
            raise Exception("Could not recognize city name")
        return (latitude - 3, longitude - 3, latitude + 3, longitude + 3)

    def _modifyRecord(self, record):
        """
            Modify the record as necessary
            1. Add geohash tag inside [Point] and [toPoint] tags based on the lat/long
        
        Args:
            record (String): A modified Json String based on original data
        """
        names = ['point', 'toPoint']
        for name in names:
            if name in record:
                record[name]['geohash'] = self._encode(record[name]['coordinates'][0], record[name]['coordinates'][1])
            
    def _encode(self, latitude, longitude, precision=12):
        """
        Taken from: https://github.com/vinsci/geohash
        Encode a position given in float arguments latitude, longitude to
        a geohash which will have the character count precision.
        """
        lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
        geohash = []
        bits = [ 16, 8, 4, 2, 1 ]
        bit = 0
        ch = 0
        even = True
        while len(geohash) < precision:
            if even:
                mid = (lon_interval[0] + lon_interval[1]) / 2
                if longitude > mid:
                    ch |= bits[bit]
                    lon_interval = (mid, lon_interval[1])
                else:
                    lon_interval = (lon_interval[0], mid)
            else:
                mid = (lat_interval[0] + lat_interval[1]) / 2
                if latitude > mid:
                    ch |= bits[bit]
                    lat_interval = (mid, lat_interval[1])
                else:
                    lat_interval = (lat_interval[0], mid)
            even = not even
            if bit < 4:
                bit += 1
            else:
                geohash += self.__base32[ch]
                bit = 0
                ch = 0
        return ''.join(geohash)
    
    def loadMapData(self, verbose=False, printWait=None, location=None):
        """
        Load Traffic Data from Bing Map API once
        
        Args:
            verbose (bool, optional): print record details
            printWait (int, optional): pause seconds when printing each record
            location (tuple): the location box for querying
        """
        count = 0
        print("Loading data from", location)
        if not location:
            self._loadBingAPI(self._getQueryBox(self.loc))
        else:
            self._loadBingAPI(self._getQueryBox(location))

        res = self._queryBingMap(self.url)
        for re in res['resourceSets'][0]['resources']:
            self._modifyRecord(re)
            if self._safeInsert(re):
                count += 1
                if verbose is True:
                    if printWait:
                        time.sleep(printWait)
                    pprint.pprint(re)
                if self.log:
                    self.logfile.write(pprint.pformat(re))
        print("Finished writing", count,  "new records")
    
    def autoLoading(self, length=1800, session=10, verbose=False, printWait=None, location=None):
        """
        Load Traffic Data from Bing Map API periodically
        DEFAULT: update collection every half an hour, last 5 hours
        
        Args:
            length (int, optional): # of seconds pause for each try 
            session (int, optional): # of tries to query data from Bing Map API
            verbose (bool, optional): print record details
            printWait (None, optional): pause seconds when printing each record
            location (tuple, optional): the location box for querying
        """
        while session > 0:
            self.loadMapData(verbose, printWait, location)
            time.sleep(length)
            session -= 1
        print("Finished writing data")

    def retrieveCol(self):
        """
        Return the collection of Helios
        This is the MAIN OUTGOING INTERFACE
        
        Returns:
            TYPE: Description
        """
        return self.col


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-f', action = 'store_true', default = False, \
                    dest = 'firstTimeSetUp', \
                    help = 'First Time Set Up, Cleaning up database and collections inside')

    parser.add_argument('-c', action = 'store_true', default = False, \
                    dest = 'confirm', \
                    help = 'Double Confirm for some actions')

    parser.add_argument('-q', action = 'store_false', default = True, \
                    dest = 'verbose', \
                    help = 'quiet model for showing stored traffic details')

    parser.add_argument('-log', action = 'store_true', default = False, \
                    dest = 'log', \
                    help = 'store log file into ./log')

    parser.add_argument('-ld', '-load', action = 'store_true', default = False, \
                    dest = 'loadMapData', \
                    help = 'Load Traffic Data from Bing Map API once')

    parser.add_argument('-al', '-autoload', action = 'store_true', default = False, \
                    dest = 'autoLoading', \
                    help = 'Load Traffic Data from Bing Map API periodically')

    parser.add_argument('-l', '-length', \
                    dest = 'length', default = 1800, type = int, \
                    help = '# of seconds pause for each query fetch')

    parser.add_argument('-s', '-session', \
                    dest = 'session', default = 10, type = int, \
                    help = '# of fetches to query data from Bing Map API')

    parser.add_argument('-p', '-printwait', \
                    dest = 'printWait', default = 5, type = int, \
                    help = '# of seconds pause for displaying traffic details')

    parser.add_argument('-loc', '-location', \
                    dest = 'location', default = 'denver', type = str, nargs='+',\
                    help = 'The location for querying, could be tuple (latitude, longitude) or city name')

    parser.add_argument('-db', '-dbName', \
                    dest = 'dbName', default = 'Helios', type = str, nargs='+',\
                    help = 'Database name for querying')

    parser.add_argument('-col', '-colName', \
                    dest = 'colName', default = 'TrafficData', type = str, nargs='+',\
                    help = 'Collection name for querying')

    args = parser.parse_args()
    args.location = ' '.join(args.location) if isinstance(args.location, list) else args.location
    args.dbName = ' '.join(args.dbName) if isinstance(args.dbName, list) else args.dbName
    args.colName = ' '.join(args.colName) if isinstance(args.colName, list) else args.colName

    print('-' * 50)
    for arg in vars(args):
        print ('{:15}'.format(arg), '\t', getattr(args, arg))
    print('-' * 50)
    # print(args.location)
    
    helios = Helios(args.firstTimeSetUp, args.confirm, args.log, args.dbName, args.colName)
    if args.loadMapData:
        helios.loadMapData(args.verbose, args.printWait, args.location)
    if args.autoLoading:
        helios.autoLoading(args.length, args.session, args.verbose, args.printWait, args.location)
