#!/usr/bin/env python3

import sys
import json

FNAME = sys.argv[1]
# for FNAME in glob.glob("A*json"):
print("fixing ", FNAME)
with open(FNAME) as fin:
    DATA = fin.read()

j = json.loads(DATA)
j["blast_params"] = json.dumps(j["blast_params"])
j["protocol"] = "1.0"

with open(FNAME + ".fix", "w") as fout:
    fout.write(json.dumps(j), indent=4)
