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
par_ageSecH=70 # for alarming
par_diffPeriodHour=1.1 # for computing averages or differentails
par_prodID='Px2id'
par_limRecCnt=200
par_plotCollName='jobs_validator'
#par_plotCollName='daq_files_watcher'

daemonL=['daq_files_watcher','submitter','jobs_validator','merger','buffers_clearner']

#daemonL=['daq_files_watcher','merger']

varTypeL=['stats','accum_stats']

# create app
appSPM = Flask(__name__)
appSPM.config.from_object(__name__)
queryCnt=0

@appSPM.route('/')
def index():
    user_id = request.environ.get( "uid" )
    return render_template('index.html', user_id = user_id, timeStartDaemon=timeStarDaemon)


#-----------
@appSPM.route('/hi/<name>')
def hi(name):
    return 'Hello33, %s!' % (name)


@appSPM.route('/plotter/<name>')
def plotter(name):
    global queryCnt, par_limRecCnt, par_plotCollName
    par_plotCollName=name
    print 'PLOTTER coll  =%s=  load new html page' % (name)
    return render_template('drawOneColl.html', collName=par_plotCollName, prodId=par_prodID)



@appSPM.route('/getStar/<name>', methods=['GET', 'POST'])
def getStar(name):
     print 'py: getStar name=',name
     return queryMongoDb_1D(name)


#---------------------
#------  uses  MongoDbUtilLean.py  to make plot(s)
#---------------------
@appSPM.route('/askFH')
def askMongoFH():

    timeSec=time.time()
    global queryCnt, par_limRecCnt, par_plotCollName
    queryCnt+=1

    DB_COLL=par_plotCollName+'_'+par_prodID



    dbInfo={}

    # deal with single collection
    print "monitoring from mongoDb coll=",DB_COLL
    watchColl = MongoDbUtil('ro').database()[DB_COLL]
    collCnt=watchColl.count()
    print 'collCnt=',collCnt, ' limRecCnt=',par_limRecCnt

 
    # get last K records
    recA=watchColl.find().sort([['_id', -1]] ).limit(par_limRecCnt)

    dbInfo['name']=DB_COLL
    dbInfo['collCnt']=collCnt
    dbSers={}
    print 'info=',dbInfo

    rec0=recA[0]
    print 'rec0=',rec0
    for varT in varTypeL:
        for vName in rec0[varT]:
            #print 'add ser=',vName
            dbSers[vName]=[]

    print 'BBB',dbSers

    for rec in recA:
        date1=rec['date']
        #print 'rec=',rec
        for varT in varTypeL:
            for vName in rec[varT]:
                point=[date1,rec[varT][vName] ]
                #print varT,rec[varT][vName]
                #print varT,point
                dbSers[vName].append(point)
            #break    

    print 'sersL=',len(dbSers)
    obj=dict(dbInfo=dbInfo, dbSers=dbSers)
    return jsonify( obj)





#---------------------
#------  uses  MongoDbUtilLean.py  to get last record 
#---------------------


def queryMongoDb_1D(starProdId ):
    global queryCnt, daemonL, varTypeL, par_prodID
    par_prodID=starProdId
    queryCnt+=1
    unixNow=datetime.datetime.utcnow();
    dbOut={}
    dbOut['info']={}
    dbOut['info']['prodId']=starProdId
   
    dbOut['daem2']=[]

    myDb = MongoDbUtil('ro').database()

    for daem in daemonL:
        collName=daem+'_'+starProdId
        print '.......monSTAR daemon=',collName
        myColl=myDb[collName]
        collCnt=myColl.count()
        daemD={}
        daemD['name']=daem
        daemD['cnt']=collCnt
        
        print 'start=',myColl, ' count=',  collCnt
        if collCnt<2:
            dbOut['daem2'].append(daemD)
            continue
        [lastRec]=myColl.find().skip(collCnt-1)
        #print 'lastRec=',lastRec

        # ......detect age of last record
        unixLastRec=lastRec['date']
        #print 'xx=', unixLastRec
        unixDiff=unixNow-unixLastRec
        #print 'last record age/sec', unixDiff,' now(utc)=',unixNow,' lastRec=',unixLastRec
        daemD['ageS']="%.1f"%unixDiff.total_seconds() 
        daemD['alarm']=unixDiff.total_seconds() >par_ageSecH

        daemD['body']={}
        
        for varT in varTypeL:
            for x in lastRec[varT]:
                daemD['body'][x]=lastRec[varT][x]
        dbOut['daem2'].append(daemD)
        print 'JJ', dbOut['daem2']
        
    print 'Py MM end, nColl=',len(daemonL),'  dbOut=',dbOut
    error='dupa2'
    print 'py: dbOut',dbOut
    return render_template('index.html', error88=error, dbInfo=dbOut['info'],dbDaem2=dbOut['daem2'] )


