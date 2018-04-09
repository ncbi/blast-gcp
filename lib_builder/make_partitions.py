#!/usr/bin/env python

dbs=["nt", "nd.02", "alligator"]
for db in dbs:
#    print "{ \"DB\": \"%s\", \"partition\": \"%s\"}" % (db,db)
    for x in range(886):
        print "{ \"DB\": \"%s\", \"part\": \"%s_50M.%0.2d\"}" % (db,db,x)

