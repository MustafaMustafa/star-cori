#!/usr/bin/env python
""" Example use of mongoDB swiss knife """

from MongoDbUtilLean import MongoDbUtil

def main():

    daqFilesWatcherColl = MongoDbUtil('ro').database()['daq_files_watcher_P16id']
    print daqFilesWatcherColl

    mx=5
    print "dump first ",mx
    for it in daqFilesWatcherColl.find().limit(mx):
        print it

if __name__ == '__main__':
    main()
