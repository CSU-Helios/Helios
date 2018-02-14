
import pymongo
import pprint
import urllib.request
import json
import time
import sys
import argparse

class Helios:

    """
    Functions:
        1. Get a instance of existing collection of Helios
        2. Retrieve traffic data from Bing Map API
    """
    
    def __cons_Names(self):
        """
        All constant valuables assignments
        """
        self.host = "localhost"
        self.port = 27017
        self.dbName = "Helios_Test"
        self.colName = "Helios_Traffic_Data"
        self.primaryKey = "incidentId"
        self.logfileName = "./log/Helios_Traffic_Data_Log-" + time.ctime().replace(" ", "-") + ".log"
        
        # Coordinates are more or less centered on Denver
        # (southLatitude, westLongitude, northLatitude, eastLongitude)
        self.loc = (35, -115, 45, -95)
        self.log = False
        
        
    def __init__(self, firstTimeSetUp = False, confirm = False, log = False):
        """
        Initiation, including load constant names, connect to Bing Map API,
        connect to an openning MongoDB, connect to Mongod database, 
        connect to Mongod collection
        If firstTimeSetUp, it will clean the existing database, named self.dbName
        Args:
            firstTimeSetUp (bool, optional): Set up Helios MongoDB for the first time
            confirm (bool, optional): Double Confirm when firstTimeSetUp
        """

        # TODO add loc para

        self.__cons_Names()
        
        self.loadBingAPI(self.loc)
        self.connectMongoDB(self.host, self.port)
        
        if firstTimeSetUp:
            if confirm:
                self.__cleanUpDatabase(self.dbName, confirm)
            else: print("ERROR: Not cleanning up database, please specific 'confirm'")
            
            
        self.connectDatabase(self.dbName)
        self.connectCollection(self.colName)
        
        if firstTimeSetUp:
            self.mongoColConf(self.primaryKey)

        if log:
            self.log = True
            self.logfile = open(self.logfileName, "w")

    def __del__(self):
        if self.log:
            self.logfile.close()
            
    def loadBingAPI(self, location):
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
        
    def connectMongoDB(self, host, port):
        """
        Connect to MongoD and create a client instance
        Args:
            host (String): the host Mongod used
            port (Int): the port Mongod listened
        """
        self.client = pymongo.MongoClient(host, port)
        
    def connectDatabase(self, datebase):
        """
        Connect to MongoD client and open a database instance
        Args:
            datebase (String): The database name for Helios
        """
        self.db = self.client[datebase]
    
    def connectCollection(self, collection):
        """
        Connect to MongoD database and open a collection instance
        Args:
            collection (String): The collection name for Helios
        """
        self.col = self.db[collection]
        
    def __cleanUpDatabase(self, databaseName = None, confirm = False):
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
                
    def __cleanUpCollection(self, collectionName = None, confirm = False):
        """
        !DANGEROUS
        Cleaning up / Removing all the collection instances in Mongod
        PRIVATE METHOD
        Args:
            collectionName (String, optional): The collection name for Helios
            Confirm (bool, optional): Double Confirm
        """
        if confirm:
            if not collectionName:
                for name in self.client.collection_names():
                    self.db.drop_collection(name)
            else:
                self.db.drop_collection(collectionName)
    

    def mongoColConf(self, primaryKey):
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
    
    def cleanUpCollection(self, confirm = False):
        """
        !DANGEROUS
        Available public method for cleanning up collections
        Args:
            confirm (bool, optional): Double Confirm
        """
        self.__cleanUpCollection(self.colName, confirm)
    
    def queryBingMap(self, url = None):
        """
        Query Bing Map API and retrieve data
        [https://msdn.microsoft.com/en-us/library/hh441726.aspx]
        Args:
            url (String, optional): Query URL
        Returns:
            TYPE: list
            [https://msdn.microsoft.com/en-us/library/hh441730.aspx]
        """
        if not url:
            url = self.url
            
        request = urllib.request.Request(url)
        response = urllib.request.urlopen(request)
        text = response.read().decode(encoding="utf-8")
        return json.loads(text)
    
    def safeInsert(self, json):
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
    
    def loadMapData(self, verbose = False, printWait = None):
        """
        Load Traffic Data from Bing Map API once
        Args:
            verbose (bool, optional): print record details
            printWait (int, optional): pause seconds when printing each record
        """
        res = self.queryBingMap(self.url)
        for re in res['resourceSets'][0]['resources']:
            if self.safeInsert(re):
                if verbose:
                    if printWait:
                        time.sleep(printWait)
                    pprint.pprint(re)
                if self.log:
                    self.logfile.write(pprint.pformat(re))
    
    def autoLoading(self, length = 1800, session = 10, verbose = False, printWait = None):
        """
        Load Traffic Data from Bing Map API periodically
        DEFAULT: update collection every half an hour, last 5 hours
        Args:
            length (int, optional): # of seconds pause for each try 
            session (int, optional): # of tries to query data from Bing Map API
            verbose (bool, optional): print record details
            printWait (None, optional): pause seconds when printing each record
        """
        while session > 0:
            self.loadMapData(verbose, printWait)
            time.sleep(length)
            session -= 1

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
                    help = 'First Time Set Up, Cleaning up database and collection')

    parser.add_argument('-c', action = 'store_true', default = False, \
                    dest = 'confirm', \
                    help = 'Double Confirm for some actions')

    parser.add_argument('-q', action = 'store_false', default = True, \
                    dest = 'verbose', \
                    help = 'quiet model for showing stored traffic details')

    parser.add_argument('-log', action = 'store_true', default = False, \
                    dest = 'log', \
                    help = 'store log file into ./log')

    parser.add_argument('-ld', "-load", action = 'store_true', default = False, \
                    dest = 'loadMapData', \
                    help = 'Load Traffic Data from Bing Map API once')

    parser.add_argument('-al', "-autoload", action = 'store_true', default = False, \
                    dest = 'autoLoading', \
                    help = 'Load Traffic Data from Bing Map API periodically')

    parser.add_argument("-length", "-l", \
                    dest = "length", default = 1800, type = int, \
                    help = "# of seconds pause for each query fetch")

    parser.add_argument("-session", "-s", \
                    dest = "session", default = 10, type = int, \
                    help = "# of fetches to query data from Bing Map API")

    parser.add_argument("-printwait", "-p", \
                    dest = "printWait", default = 5, type = int, \
                    help = "# of seconds pause for displaying traffic details")

    args = parser.parse_args()
    
    helios = Helios(args.firstTimeSetUp, args.confirm, args.log)
    if args.loadMapData:
        helios.loadMapData(args.verbose, args.printWait)
    if args.autoLoading:
        helios.autoLoading(args.length, args.session, args.verbose, args.printWait)

