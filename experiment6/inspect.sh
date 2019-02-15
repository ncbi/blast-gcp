
LOG=$1
PLOT=$LOG.plot
STARTX="$2"
ENDX="$3"

#sort the log-file numerically
sort $LOG -n -o $LOG

#transform the log-file with python script into the plot-file
rm $PLOT
./transform.py $LOG $PLOT $STARTX $ENDX

#sort the plot-file numerically
sort $PLOT -n -o $PLOT

#call gnuplot to visualize it, by calling the gnu-plot-script 'vector.gpl'
gnuplot -persist -e "file='$PLOT'; startx=$STARTX; endx=$ENDX" vector.gpl

