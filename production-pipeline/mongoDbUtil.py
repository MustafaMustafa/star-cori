#!/bin/usr/python
__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

"""
Utility class to handle connections to mongoDb and fetching collections
"""

import logging
import time
import pymongo
from pymongo import MongoClient

class mongoDbUtil:
    """Usage e.g. 
       m = mongoDbUtil('admin')
       coll = m.db['myCollection']"""

    logging.basicConfig(level=logging.INFO)

    def __init__(self, user = 'readOnly', dbName = 'STARProdState', dbServer='mongodb01.nersc.gov'):

        if user == 'admin':
            self._user = 'STARProdState_admin'
            self._password = 'w2e23sddf21'
        else:
            self._user = 'STARProdState_ro'
            self._password = 'w3s42dwq42'

        self._dbName = dbName
        self._dbServer = dbServer

        self._connectDb()

    def _connectDb(self):

        logging.info("Connecting to %s@%s. User: %s ..."%(self._dbServer, self._dbName, self._user))
        connectionTimeOutMax = 5
        try:
            self._client = MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(self._user, self._password, self._dbServer, self._dbName), 
                                        serverSelectionTimeoutMS = connectionTimeOutMax)
            self._client.server_info() # attempt a connection
            self.db = self._client[self._dbName]

        except pymongo.errors.ServerSelectionTimeoutError as err:
            logging.error("ERROR: Could not connect to DB server ...")
