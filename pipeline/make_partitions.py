#!/usr/bin/env python

dbs=["nt.04", "nd.02", "alligator"]
for db in dbs:
    print "{ \"DB\": \"%s\", \"partition\": \"%s\"}" % (db,db)
    for x in range(100):
        print "{ \"DB\": \"%s\", \"partition\": \"%s_%0.2d\"}" % (db,db,x)

