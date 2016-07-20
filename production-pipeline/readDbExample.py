#!/usr/bin/env python
""" Example use of mongoDB swiss knife """

from MongoDbUtil import MongoDbUtil

def main():

    daqFilesWatcherColl = mongoDbUtil('ro').db['daqFilesWatcher']
    print daqFilesWatcherColl

    for it in daqFilesWatcherColl.find():
        print it

if __name__ == '__main__':
    main()
