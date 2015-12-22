#!/usr/bin/env python 

import json
import requests
import psutil
import time
import argparse
import setproctitle
import random
import subprocess
import os

FNULL = open(os.devnull, 'w')

parser = argparse.ArgumentParser()
parser.add_argument('--url', default="172.28.153.53", type=str)
parser.add_argument('--port', default=55482, type=int)
parser.add_argument('--instances', default=5, type=int)
parser.add_argument('--keys', default=100, type=int)
parser.add_argument('--timeout', default=100, type=int)
args = parser.parse_args()

setproctitle.setproctitle('bombay-test-random')


port = args.port
altport = args.port + args.instances + 1

#start bombay instances
for i in xrange(args.instances):
    pxargs = ["./bombay", "--port", str(port), "--dataport", str(altport)]
    if i != 0:
        pxargs.append("--joinaddr")
        pxargs.append(str(args.url))
        pxargs.append("--joinport")
        pxargs.append(str(altport-1))
    subprocess.Popen(pxargs, stdout=FNULL, stderr=subprocess.STDOUT)
    port += 1
    altport += 1
    time.sleep(1)

put_req = {
                    "key" : "",
                    "value": "abcdefghijklmnopqrstuvwxyz",
                    "space_req": 0,
                    "uptime": 0.8,
                    "avg_bandwidth": 1000, #mb/s
                    "peak_bandwidth": 1000, #mb/s
                    "durability_time": 10000,
                    "durability_percentage": 0.9
                  }

t0 = time.clock()

rport = args.port

s = requests.session()
s.keep_alive = False

for kk in xrange(args.keys):
    put_req['uptime'] = random.random()
    put_req['key'] = str(random.randint(1,1000))
    rport += 1
    if rport > port-1:
        rport = args.port
    url = "http://" + "127.0.0.1" + ':' + str(rport) + '/put'
    didwork = False
    while not didwork:
        didwork= True
        try:
            r = requests.post(url, json=put_req)
        except requests.exceptions.RequestException as e:
            print e
            didwork = False
            time.sleep(float(args.timeout)/1000.0)
    #print r.text
    #time.sleep(float(args.timeout)/1000.0)

teatime = time.clock() - t0

subprocess.Popen(["sudo", "killall", "bombay"])

subprocess.Popen(["sudo", "killall", "alternator"])

subprocess.Popen(["sudo", "killall", "talese"])

time.sleep(1)

print "Time taken: " + str(teatime) + " seconds for " + str(args.keys) + " keys over " + str(args.instances) + " machines." 