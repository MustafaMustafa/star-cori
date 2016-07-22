#!/usr/bin/env python

from flup.server.fcgi import WSGIServer

from appStarProdMon import appSPM

if __name__ == "__main__" :
    WSGIServer( appSPM, bindAddress = ( "127.0.0.1", 8876 ) ).run()
