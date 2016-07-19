#!/bin/usr/python
__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

"""
Utility class to handle connections to mongoDb and fetching collections
"""

import logging
import pymongo
from pymongo import MongoClient

class mongoDbUtil:
    """Usage e.g. 
       m = mongoDbUtil('admin')
       coll = m.db['myCollection']"""

    logFormat = '%(asctime)-15s %(levelname)s: %(message)s'
    logging.basicConfig(level=logging.INFO, format=logFormat)

    def __init__(self, user = 'readOnly', dbName = 'STARProdState', dbServer='mongodb01.nersc.gov'):

        if user == 'admin':
            self.__user = 'STARProdState_admin'
            self.__password = 'w2e23sddf21'
        else:
            self.__user = 'STARProdState_ro'
            self.__password = 'w3s42dwq42'

        self.__dbName = dbName
        self.__dbServer = dbServer

        self.__connectDb()

    def __connectDb(self):

        logging.info("Connecting to %s@%s. User: %s ..."%(self.__dbServer, self.__dbName, self.__user))
        connectionTimeOutMax = 5
        try:
            self.__client = MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(self.__user, self.__password, self.__dbServer, self.__dbName), 
                                        serverSelectionTimeoutMS = connectionTimeOutMax)
            self.__client.server_info() # attempt a connection
            self.db = self.__client[self.__dbName]

        except pymongo.errors.ServerSelectionTimeoutError as err:
            logging.error("ERROR: Could not connect to DB server ...")
