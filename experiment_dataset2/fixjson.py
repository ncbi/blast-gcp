#!/usr/bin/env python3.6

import os
import glob
import json

for fname in glob.glob("A*json"):
    print (fname)
    with open(fname) as fin:
        data=fin.read()

    j=json.loads(data)
    print (j)
    j['blast_params']=json.dumps(j['blast_params'])

    with open(fname, "w") as fout:
        fout.write(json.dumps(j))
