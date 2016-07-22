#!/usr/bin/env python
""" Example use of mongoDB swiss knife """

from MongoDbUtilJan import MongoDbUtil

def main():

    daqFilesWatcherColl = MongoDbUtil('ro').database()['daq_files_watcher_P16id']
    print daqFilesWatcherColl

    for it in daqFilesWatcherColl.find():
        print it

if __name__ == '__main__':
    main()
