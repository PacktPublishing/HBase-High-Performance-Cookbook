__author__ = 'r0choud'
#import thrift libraries
#! /usr/bin/env python

import sys
import os
import time

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

from hbase import THBaseService
from hbase.ttypes import *

# Add path for local "gen-py/hbase" for the pre-generated module
gen_py_path = os.path.abspath('gen-py')
sys.path.append(gen_py_path)

print "Connecting from Python to HBase using thrift"

host = "172.28.182.45" # Hbase Master to connect
port = 9090 # Port to connect to
framed = False
try:
    socket = TSocket.TSocket(host, port) # Passing host and post details to the R=TSocket
    if framed:
         transport = TTransport.TFramedTransport(socket)
    else:
        transport = TTransport.TBufferedTransport(socket)

    protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # Getting Thrift client handle and instance
    client = THBaseService.Client(protocol)

        #Opening socket Connection with HBase
    transport.open()

    table = "mywebproject:mybrowsedata" # passing the table details

    # Creating PUT call by using TPut
    put = TPut(row="row02", columnValues=[TColumnValue(family="web",qualifier="site",value="www.yahoo.com")])
    print "Putting Data to the web column family:", put

    # Put Data in HBase using thrift client
    client.put(table, put)

    # Create the GET call
    get = TGet(row="row02")
    print "Retrieving Data :", get

    # Retrieve Data from HBase using thrift client
    result = client.get(table, get)

    print "Result:", result

    #Closing socket connection with HBase
    transport.close()
except IOError, e:
       if hasattr(e, 'code'): # Error
         print 'error code: ', e.code
       elif hasattr(e, 'reason'): # Error
        print "can't connect, reason: ", e.reason
       else:
         raise
finally:
       transport.close() # closing the connection in finally section to make sure the connection is always closed
