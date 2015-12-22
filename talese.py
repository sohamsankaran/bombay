#!/usr/bin/env python 

import json
import requests
import psutil
import time
import argparse
import setproctitle

parser = argparse.ArgumentParser()
parser.add_argument('--url', default="http://127.0.0.1", type=str)
parser.add_argument('--port', default="1337", type=str)
parser.add_argument('--timeout', default=10, type=int)
args = parser.parse_args()

setproctitle.setproctitle('talese')

url = args.url + ':' + args.port + '/updateprofile'


node_properties = {
                    "space_total": "",
                    "space_remaining": "",
                    "uptime": 0.95,
                    "avg_bandwidth": 1000, #mb/s
                    "peak_bandwidth": 1000, #mb/s
                    "seq_read_speed": "",
                   "rand_read_speed": "",
                    "failure_rate": 0.01
                  }

while (1):

        node_properties['space_total'] = psutil.disk_usage('/').total

        node_properties['space_remaining'] = psutil.disk_usage('/').free

        d_ioc = psutil.disk_io_counters()

        node_properties['rand_read_speed'] = d_ioc.read_bytes/d_ioc.read_time

        node_properties['seq_read_speed'] = d_ioc.read_bytes/d_ioc.read_time #TODO actually test sequential read speed

        #print json.dumps(node_properties)

        r = requests.post(url, json=node_properties)

        print r

        time.sleep(args.timeout)
        
