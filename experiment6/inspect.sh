
LOG=$1
PLOT=$LOG.plot
STARTX="0"
ENDX="500"

#sort the log-file numerically
sort $LOG -n -o $LOG

#transform the log-file with python script into the plot-file
./transform.py $LOG $PLOT

#sort the plot-file numerically
sort $PLOT -n -o $PLOT

#call gnuplot to visualize it, by calling the gnu-plot-script 'vector.gpl'
gnuplot -persist -e "file='$PLOT'; startx=$STARTX; endx=$ENDX" vector.gpl

