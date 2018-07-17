#! /usr/bin/env python

import sys
import os

#=================================================================================
class color_pick:
	def __init__( self ):
		#green, blue, light-blue, orange, purple, magenta, pink, light-green,
		#dark-green, teal, dark-purple, yellow, dark-green, dark-blue, dark-purple, light-purple
		self.idx = 0
		self.colors = { 0:"0x06ff02", 1:"0x0224ff", 2:"0x02d0ff", 3:"0x0ff9a02", \
			4:"0x06702ff", 5:"0x0ff02e9", 6:"0xf771c6", 7:"0x0b4f771", \
			8:"0x05b773f", 9:"0x03f7772", 10:"0x0413f77", 11:"0x0eeff00", \
			12:"0x0225900", 13:"0x0c59", 14:"0x04d0059", 15:"0x0be89ff" }

	def pick( self ):
		c = self.colors.get( self.idx, None )
		if c == None :
			self.idx = 0
			c = self.colors[ 0 ]
		self.idx += 1
		return c


#=================================================================================
class req_colors:
	def __init__( self ):
		self.color_picker = color_pick();
		self.d = {}

	def pick( self, req_id ):
		c = self.d.get( req_id, None )
		if c == None :
			c = self.color_picker.pick()
			self.d[ req_id ] = c
		return c

#=================================================================================
class vector:
	def __init__( self, start_time, duration, executor_name, label, color ):
		self.start_time = start_time
		self.duration = duration
		self.executor_name = executor_name
		self.y = 0
		self.label = label
		self.color = color

	def write_to( self, plot, line_nr ) :
		line = "%d %d %s %d %s %s\n" % ( self.start_time, self.duration, self.executor_name, line_nr + self.y, self.label, self.color )
		plot.write( line )
		plot.flush()


#=================================================================================
class executor_sub:
	def __init__( self, offset ):
		self.vectors = []
		self.offset = offset

	def add( self, vector ) :
		self.vectors.append( vector )

#=================================================================================
class executor_line:
	def __init__( self, y, x, idx ):
		self.y = y
		self.x = x
		self.idx = idx

#=================================================================================
class executor:
	def __init__( self ):
		self.vectors = []

	def add( self, vector ) :
		self.vectors.append( vector )

	def spread( self, factor ) :
		lines = []
		res = 0
		for v in self.vectors:		
			l = len( lines )
			if l == 0 :
				#we do not have to adjust the vector, just record its end-point
				lines.append( executor_line( 0, v.start_time + v.duration, 0 ) )
			else :
				#find the line that has the shortest distance between its end-point
				#and the start of the vector, adjust endpoint of that line
				distance = sys.maxint
				idx = 0
				y = 0
				for el in lines :
					d = v.start_time - el.x
					if d < distance and d > 0 :
						distance = d
						idx = el.idx
						y = el.y
				if distance < sys.maxint :
					#we found a line to use
					v.y = y
					lines[ idx ].x = v.start_time + v.duration
				else :
					#we did not find a line to use, add another one
					v.y = l * factor
					v.idx = l
					lines.append( executor_line( v.y, v.start_time + v.duration, v.idx ) )
				if v.y > res :
					res = v.y
		return res

	def write_to( self, plot, line_nr, factor ) :
		num_lines = self.spread( factor )
		for v in self.vectors:
			v.write_to( plot, line_nr )
		return line_nr + num_lines + factor


#=================================================================================
class vectors:
	def __init__( self ):
		self.executors = {}

	def add( self, vector ) :
		e = self.executors.get( vector.executor_name, None )
		if e == None :
			e = executor()
			self.executors[ vector.executor_name ] = e
		e.add( vector )

	def write_to( self, plot, factor ) :
		line_nr = factor
		for key in sorted( self.executors ) :
			line_nr = self.executors[ key ].write_to( plot, line_nr, factor )


#=================================================================================
def transform( event_file_name ):
	res = vectors()
	start_times = {}
	colors = req_colors();
	lowest_time = 0
	events = open( event_file_name )
	for line in events:
		#split the line into fields:
		a = line.strip().split()
		#a[0] ... timestamp in ms
		#a[1] ... worker-name
		#a[2] ... executor-number
		#a[3] ... event-type ( 'S'=start, 'D'=done )
		#a[4] ... how many traceback-list produced ( for event 'D' )
		#a[5] ... volume-name
		#a[6] ... request-id

		if lowest_time < 1 :
			lowest_time = int( a[0] )

		executor_name = "%s-%s" % ( a[1], a[2] )
		vector_label = "%s-%s" % ( a[5], a[6] )
		key = "%s-%s" % ( executor_name, vector_label )

		if a[3] == "S" :
			start_times[ key ] = a[ 0 ]
		elif a[3] == "D" :
			start = start_times.get( key, None )
			if start == None :
				print "oops done without start! : %s" % a
			else :
				start_time = int( start )
				duration = int( a[ 0 ] ) - start_time
				start_time -= lowest_time
				color = colors.pick( a[6] )
				label = "%s-%s" %( vector_label, a[4] )
				res.add( vector( start_time, duration, executor_name, label, color ) )
				start_times.pop( key )
	return res
			
#=================================================================================
if __name__ == '__main__':
    if len( sys.argv ) > 2 :
        event_file_name = sys.argv[ 1 ]
        if os.path.isfile( event_file_name ) :
			plot_file_name = sys.argv[ 2 ]
			print "transforming '%s' ---> '%s'" % ( event_file_name, plot_file_name )
			v = transform( event_file_name )
			v.write_to( open( plot_file_name, "w" ), 2 )
        else :
            print "'%s' does not exist!" % event_file_name
    else :
        print "file-names are missing!"
        print "usage: transform.py events.log gnuplot.dat"

