"""A class for a hearbeat daemon that updates the DB with stats on every beat"""

import threading
import time
import datetime
import logging

class StatsHeartbeat(object):
    """A class for a hearbeat daemon that updates the DB with stats on every beat"""

    __logger = logging.getLogger()

#pylint: disable-msg=too-many-arguments
    def __init__(self, heartbeat_interval, hearbeat_coll, accum_stats, stats, verbose=False):
        self.__heartbeat_interval = heartbeat_interval
        self.accum_stats = accum_stats
        self.stats = stats
        self.__verbose = verbose
        self.__db_coll = hearbeat_coll
        self.__beat_your_heart = True

        self.__init_stats()

        heartbeat_thread = threading.Thread(target=self.__heartbeat)
        heartbeat_thread.setDaemon(True)
        heartbeat_thread.start()

#pylint: enable-msg=too-many-arguments

    def __init_stats(self):
        """Intialize stats from DB latest record"""

        self.__logger.info("Initializing variables from DB ...")

        if self.__db_coll.count():
            last_doc = self.__db_coll.find().skip(self.__db_coll.count()-1)[0]
            self.accum_stats = last_doc['accum_stats']
            self.stats = last_doc['stats']

        self.__print_stats()

    def __heartbeat(self):
        """Send a heartbeat to DB """

        time.sleep(self.__heartbeat_interval)
        while self.__beat_your_heart:
            entry = {'accum_stats': self.accum_stats,
                     'stats': self.stats,
                     'date' : datetime.datetime.utcnow()}
            self.__db_coll.insert(entry)

            if self.__verbose:
                self.__print_stats()

            time.sleep(self.__heartbeat_interval)

    def __print_stats(self):
        for key in self.accum_stats.keys():
            self.__logger.info("%s according to DB = %i", key.replace('_', ' '), self.accum_stats[key])

        for key in self.stats.keys():
            self.__logger.info("%s according to DB = %i", key.replace('_', ' '), self.stats[key])
