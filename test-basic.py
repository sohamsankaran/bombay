#!/usr/bin/env python 

import json
import requests
import psutil
import time
import argparse
import setproctitle
import random

parser = argparse.ArgumentParser()
parser.add_argument('--url', default="http://127.0.0.1", type=str)
parser.add_argument('--port', default="1337", type=str)
parser.add_argument('--timeout', default=10, type=int)
args = parser.parse_args()

setproctitle.setproctitle('bombay-test-basic')

url = args.url + ':' + args.port + '/put'


put_req = {
                    "key" : "",
                    "value": "gg",
                    "space_req": 0,
                    "uptime": 0.95,
                    "avg_bandwidth": 1000, #mb/s
                    "peak_bandwidth": 1000, #mb/s
                    "durability_time": 10000,
                    "durability_percentage": 0.9
                  }

while (1):

        put_req['key'] = str(random.randint(1,1000))
        r = requests.post(url, json=put_req)

        print r

        time.sleep(args.timeout)
        
