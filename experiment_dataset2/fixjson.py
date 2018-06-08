#!/usr/bin/env python3.6

import sys
import os
import glob
import json

fname=sys.argv[1]
#for fname in glob.glob("A*json"):
print ("fixing ", fname)
with open(fname) as fin:
    data=fin.read()

j=json.loads(data)
j['blast_params']=json.dumps(j['blast_params'])
j['protocol']="1.0"

if False:
    if 'db' in j['blast_params']:
        j['db']=j['blast_params']['db']
        j['db_tag']=j['blast_params']['db']

    if 'program' in j['blast_params']:
        j['program']=j['blast_params']['program']

    if 'queries' in j['blast_params']:
        j['query_seq']=j['blast_params']['queries'][0]

    if 'queries' in j:
        j['query_seq']=j['queries'][0]

with open(fname+".fix", "w") as fout:
    fout.write(json.dumps(j))
