#!/bin/usr/python
"""
Utility class to handle connections to mongoDb and fetching collections
"""

import logging
import pymongo
from pymongo import MongoClient

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

class MongoDbUtil(object):
    """Usage e.g.
       m = mongoDbUtil('admin')
       coll = m.db['myCollection']"""

    logFormat = '%(asctime)-15s %(levelname)s: %(message)s'
    logging.basicConfig(level=logging.INFO, format=logFormat)

    def __init__(self, user='readOnly', db_name='STARProdState', db_server='mongodb01.nersc.gov'):

        if user == 'admin':
            self.__user = 'STARProdState_admin'
            self.__password = 'w2e23sddf21'
        else:
            self.__user = 'STARProdState_ro'
            self.__password = 'w3s42dwq42'

        self.__db_name = db_name
        self.__db_server = db_server

        self.__connect_db()

    def __connect_db(self):
        """Establish connection to DB
        """

        logging.info("Connecting to %s@%s. User: %s ...", self.__db_server, self.__db_server, self.__user)
        connection_timeout_max = 5
        try:
            self.__client = MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(self.__user, self.__password, self.__db_server, self.__db_name),
                                        serverSelectionTimeoutMS=connection_timeout_max)
            self.__client.server_info() # attempt a connection
            self.__database = self.__client[self.__db_name]

        except pymongo.errors.ServerSelectionTimeoutError:
            logging.error("ERROR: Could not connect to DB server ...")
            exit(1)

    def database(self):
        """Return DB"""
        return self.__database
