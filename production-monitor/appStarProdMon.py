# imports
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash, jsonify

import os, time, datetime, base64, sys, math
from bson.objectid import ObjectId

# new Mustafa mongoDB I/O
sys.path.append(os.path.abspath("../production-pipeline/"))
from MongoDbUtilLean import MongoDbUtil

current_time = time.localtime()
timeStarDaemon=time.strftime('%a, %d %b %Y %H:%M:%S ', current_time)
par_ageSecH=150 # for alarming
par_diffPeriodHour=1.1 # for computing averages or differentails
 
# create app
appSPM = Flask(__name__)
appSPM.config.from_object(__name__)
queryCnt=4

@appSPM.route('/')
def index():
    user_id = request.environ.get( "uid" )
    return render_template('index.html', user_id = user_id, timeStartDaemon=timeStarDaemon)


#---------------------
#------  uses  MongoDbUtilLean.py 
#---------------------
@appSPM.route('/m')
def getM():
    global queryCnt
    queryCnt+=1
    
    DB_COLL='daq_files_watcher_Jan'
    TOTVARN='total_files_seen'
    AVRVARN='total_files_on_disk'

    dbData={}
    dbData['subm']={}

    # deal with single collection

    print "monitoring from mongoDb coll=",DB_COLL
    watchColl = MongoDbUtil('ro').database()[DB_COLL]
    collCnt=watchColl.count()
    [last]=watchColl.find().skip(collCnt-1)

    print collCnt,last

    # ......detect age of last record
    unixNow=datetime.datetime.utcnow();
    unixLastRec=last['date']
    #print 'xx=', unixLastRec
    unixDiff=unixNow-unixLastRec
    print 'last record age/sec', unixDiff,' now(utc)=',unixNow,' lastRec=',unixLastRec

    #...... compute differential increment over set time inetrval
    
    gen_time = datetime.datetime.utcnow() - datetime.timedelta(hours=par_diffPeriodHour) 
    dummy_id = ObjectId.from_datetime(gen_time)
    result = list(watchColl.find({"_id": {"$gte": dummy_id}}))

    print 'seen records =', len(result),  'in last set period of ',datetime.timedelta(hours=par_diffPeriodHour),' hh:mm:ss'

    nr=len(result)
    delT=999999
    delSeenFile=0
    if nr>0:
        delT=result[nr-1]['date'] - result[0]['date']
        delSeenFile=result[nr-1][TOTVARN] - result[0][TOTVARN]
    print 'two records diff:  delT=',delT, ' delSeenFile=',delSeenFile


    if nr>1:    #.... average quantity for last period
        sum1=0.
        sum2=0.
        for rec in result:
            val=rec[AVRVARN]
            sum1+=val
            sum2+=val*val
        
        print 'compute avr, sum1=',sum1,' sum2=',sum2
        avrVal=sum1/nr
        sigVal=math.sqrt(sum2/nr - avrVal*avrVal)
        print AVRVARN, ' avr=%.1f  +/- sig=%.1f'%(avrVal,sigVal)
        
    #... fill output dict
    dbData['queryCnt']=queryCnt
    dbData['watch']={}
    dbData['watch']['coll']=DB_COLL
    dbData['watch']['alarm']= unixDiff.total_seconds() >par_ageSecH
    dbData['watch']['count']=collCnt
    dbData['watch']['ageSec']=int(unixDiff.total_seconds())
    dbData['watch']['seenFiles']=last[TOTVARN]
    dbData['watch']['delSeenFiles']=delSeenFile
    dbData['watch']['discFiles']=last[AVRVARN]
    dbData['watch']['avrDiscFiles']='%.0f  +/- %.0f'%(avrVal,sigVal)


    error = ''
    print 'dbData=',dbData

    return render_template('starMonVer2.html', error=error, timeStartDaemon=timeStarDaemon , dbData=dbData, hearbeatTimeoutSec=par_ageSecH,  diffPeriodHour=par_diffPeriodHour)




if __name__ == '__main__':
    app.run()
