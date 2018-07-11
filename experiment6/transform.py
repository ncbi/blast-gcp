#! /usr/bin/env python

import sys
import os


#=================================================================================
def transform( event_file_name, plot_file_name ) :
	print "transforming '%s' ---> '%s'" % ( event_file_name, plot_file_name )
	d = {}
	line_ids = {}
	lowest_time = 0
	next_line_id = 1
	with open( event_file_name ) as f_in:
		with open( plot_file_name, "w" ) as f_out :
			for line in f_in:
				a = line.strip().split()
				#key = worker-executor-reqest-volume
				key = "%s-%s-%s-%s" % ( a[1], a[2], a[5], a[6] )
				if lowest_time < 1 :
					lowest_time = int( a[0] )
				event = a[3]
				if event == "S" :
					d[ key ] = a[ 0 ]
				else :
					start = d.get( key, None )
					if start == None :
						print "oops done without start! : %s" % a
					else :
						start_time = int( start )
						duration = int( a[ 0 ] ) - start_time
						start_time = start_time - lowest_time
						line_name = "%s-%s" %( a[1], a[2] )
						line_id = line_ids.get( line_name, None )
						if line_id == None :
							line_id = next_line_id
							next_line_id += 1
							line_ids[ line_name ] = line_id
						vol_req = "%s-%s" %( a[5], a[6] )
						color = "0x0FF8080"
						plotline = "%d %d %s %d %s %s\n" % ( start_time, duration, line_name, line_id, vol_req, color )
						f_out.write( plotline )
						d.pop( key )
			f_out.close()
		f_in.close()

#=================================================================================
if __name__ == '__main__':
    if len( sys.argv ) > 2 :
        event_file_name = sys.argv[ 1 ]
        if os.path.isfile( event_file_name ) :
        	plot_file_name = sys.argv[ 2 ]
          	transform( event_file_name, plot_file_name )
        else :
            print "'%s' does not exist!" % event_file_name
    else :
        print "file-names are missing!"
        print "usage: transform.py events.log gnuplot.dat"

