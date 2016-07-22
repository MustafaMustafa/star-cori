#!/usr/bin/env python
""" Example use of mongoDB swiss knife """

import  sys, os
import datetime

sys.path.append(os.path.abspath("../production-pipeline/"))
# prints content of all searched directories, including PYTHONPATH
#print sys.path

    
from MongoDbUtilLean import MongoDbUtil

def main():

    dbColl = MongoDbUtil('ro').database()['daqFilesWatcher']
    print dbColl

    mxN=dbColl.count()
    k=0
    nShow=4
    print 'show  last ',nShow, ' records of ',mxN
    for it in dbColl.find().skip(mxN-nShow):
        if k==0:
            print it
        k+=1    
        print k, it['_id'], it['date'], it['totalNumberOfFilesSeen']



    from bson.objectid import ObjectId
    mins = 20
    gen_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=mins) 
    dummy_id = ObjectId.from_datetime(gen_time)
    result = list(dbColl.find({"_id": {"$gte": dummy_id}}))


    print 'seen records=', len(result),  'in last ',datetime.timedelta(minutes=mins), 'minutes'

    nr=len(result)
    if nr <=0:
        print "cant do math with no records, abort"
        exit(1)
    delT=result[nr-1]['date'] - result[0]['date']
    delFile=result[nr-1]['totalNumberOfFilesSeen'] - result[0]['totalNumberOfFilesSeen']
    print 'two records diff:  delT=',delT, ' delFile=',delFile
    
    unixNow=datetime.datetime.utcnow();
    unixLastRec=result[nr-1]['date']
    unixDiff=unixNow-unixLastRec
    difSec=unixDiff.total_seconds()
    print 'last record age/sec',difSec, ' hh:mm:ss=',unixDiff,' now(utc)=',unixNow,' lastRec=',unixLastRec




if __name__ == '__main__':
    main()
