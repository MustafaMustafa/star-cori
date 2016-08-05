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
daemonL=['daq_files_watcher','submitter','jobs_validator','merger','buffers_clearner']
varTypeL=['stats','accum_stats']


@appSPM.route('/')
def index():
    user_id = request.environ.get( "uid" )
    return render_template('index.html', user_id = user_id, timeStartDaemon=timeStarDaemon)


#-----------
@appSPM.route('/hi/<name>')
def say_hi(name):
    return 'Hello33, %s!' % (name)


@appSPM.route('/getStar_Jan', methods=['GET', 'POST'])
def getStar_Jan():
    print 'py-getStar_Jan begin'
    starProdId='Jan'
    print 'Py M: selected STAR prod=',starProdId
    return queryMongoDb_1D(starProdId )


@appSPM.route('/getStar_Px0id', methods=['GET', 'POST'])
def getStar_Px0id():
    print 'py-getStar_Px0id begin'
    starProdId='Px0id'
    print 'Py M: selected STAR prod=',starProdId
    return queryMongoDb_1D(starProdId )

@appSPM.route('/getStar_Px1id', methods=['GET', 'POST'])
def getStar_Px1id():
    starProdId='Px1id'
    print 'Py M: selected STAR prod=',starProdId
    return queryMongoDb_1D(starProdId )


#---------------------
#------  uses  MongoDbUtilLean.py 
#---------------------


def queryMongoDb_1D(starProdId ):
    global queryCnt, daemonL, varTypeL
    queryCnt+=1
    unixNow=datetime.datetime.utcnow();
    dbOut={}
    dbOut['info']={}
    dbOut['info']['prodId']=starProdId
    dbOut['daem']={}

    myDb = MongoDbUtil('ro').database()

    for daem in daemonL:
        collName=daem+'_'+starProdId
        print '---------monSTAR daemon=',collName
        myColl=myDb[collName]
        collCnt=myColl.count()
        dbOut['daem'][daem+'_cnt']=collCnt
        print 'start=',myColl, ' count=',  collCnt
        if collCnt<2:
            continue
        [last]=myColl.find().skip(collCnt-1)
        #print 'last=',last

        # ......detect age of last record
        unixLastRec=last['date']
        #print 'xx=', unixLastRec
        unixDiff=unixNow-unixLastRec
        #print 'last record age/sec', unixDiff,' now(utc)=',unixNow,' lastRec=',unixLastRec

        dbOut['daem'][daem+'_last_date']=last['date']
        for varT in varTypeL:
            for x in last[varT]:
                dbOut['daem'][daem+'_'+x]=last[varT][x]

    print 'Py MM end, nColl=',len(daemonL),'  dbOut=',dbOut
    error='dupa2'
    print 'py: dbOut',dbOut
    return render_template('index.html', error88=error, dbInfo=dbOut['info'], dbDaem=dbOut['daem'])




def queryMongoDb_tree(starProdId ):
    global queryCnt, daemonL, varTypeL
    queryCnt+=1
    unixNow=datetime.datetime.utcnow();
    dbOut={}
    dbOut['info']={}
    dbOut['info']['prodId']=starProdId

    myDb = MongoDbUtil('ro').database()

    for daem in daemonL:
        collName=daem+'_'+starProdId
        print '---------monSTAR daemon=',collName
        dbOut[daem]={}
        myColl=myDb[collName]
        collCnt=myColl.count()
        dbOut[daem]['cnt']=collCnt
        print 'start=',myColl, ' count=',  collCnt
        if collCnt<2:
            continue
        [last]=myColl.find().skip(collCnt-1)
        #print 'last=',last

        # ......detect age of last record
        unixLastRec=last['date']
        #print 'xx=', unixLastRec
        unixDiff=unixNow-unixLastRec
        #print 'last record age/sec', unixDiff,' now(utc)=',unixNow,' lastRec=',unixLastRec


        lastD={}
        lastD['date']=last['date']
        lastD['ageSec']=int(unixDiff.total_seconds())
        lastD['alarm']= unixDiff.total_seconds() >par_ageSecH
        for varT in varTypeL:
            lastD[varT]={}
            for x in last[varT]:
                 lastD[varT][x]=last[varT][x]

        dbOut[daem]['last']=lastD


    print 'Py MM end, nColl=',len(daemonL),'  dbOut=',dbOut
    error='dupa2'
    print 'py: dbOut',dbOut
    return render_template('index.html', error88=error, dbOut=dbOut)






if __name__ == '__main__':
    app.run()
