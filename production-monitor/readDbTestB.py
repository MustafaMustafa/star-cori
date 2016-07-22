#!/usr/bin/env python
""" dump last K records for all collections of interest """

import  sys, os
import datetime

sys.path.append(os.path.abspath("../production-pipeline/"))
# prints content of all searched directories, including PYTHONPATH
#print sys.path

from MongoDbUtilLean import MongoDbUtil


def anaOneColl(collName,nShow):

    dbColl = MongoDbUtil('ro').database()[collName]
    #print dbColl

    
    mxN=dbColl.count()
    k=0

    [rec]=dbColl.find().skip(mxN-1)
    
    unixLastRec=rec['date']
    unixDiff=unixNow-unixLastRec
    diffSec=unixDiff.total_seconds()
    print 'Last record=',mxN,'  age (sec)',int(diffSec), ' hh:mm:ss=',unixDiff,
    for x in rec:
        print '   ',x,':',rec[x]
    
    print 'show  last ',nShow, ' records of ',mxN                
    for rec in dbColl.find().skip(mxN-nShow):           
        k+=1    
        if k==1:
            print k,  rec['date']
        else:
            unixDiff=rec['date']-unixLastRec
            print k,  rec['date'], int(unixDiff.total_seconds())
        unixLastRec=rec['date']

    from bson.objectid import ObjectId
    mins = 20
    gen_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=mins) 
    dummy_id = ObjectId.from_datetime(gen_time)
    result = list(dbColl.find({"_id": {"$gte": dummy_id}}))

    print 'seen records=', len(result),  'in last ',datetime.timedelta(minutes=mins), 'minutes'

    
  


#==============================
#    M A I N
#==============================
if __name__ == '__main__':

    myCollL=['daqFilesWatcher','daq_files_watcher_Jan']
    nShow=5
    unixNow=datetime.datetime.utcnow();
    print ' now(utc)=',unixNow
    print "*** scan collections:",myCollL
    print 'pick nShow=',nShow
    
    for coll in myCollL:
        print 
        print '---- work on coll=',coll
        anaOneColl(coll,nShow)
        #break
    
   
