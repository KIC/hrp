if (!exists("outterm")) outterm='jpeg'
set terminal outterm size 1024,2048

set title font "Arial,11"
set tics font "Arial,8"
set key font "Arial,8"

set datafile separator ","
stat filename using (lastHRP=$4) skip 1 nooutput
stat filename using (lastMV=$7) skip 1 nooutput
stat filename using (lastOoN=$2) skip 1 nooutput
stat filename using 5 skip 1 nooutput name "HRP"
stat filename using 6 skip 1 nooutput name "MV"
stat filename using 3 skip 1 nooutput name "OON"

set xdata time
set timefmt "%s"

set multiplot layout 4,1 rowsfirst

set title 'Performance'
plot filename using "ROW":4  title sprintf("HRP %0.2f\%", lastHRP) with lines, \
     filename using "ROW":7  title sprintf("MV %0.2f\%", lastMV) with lines, \
     filename using "ROW":2  title sprintf("1/N %0.2f\%", lastOoN) with lines

set title 'Daily Value at Risk (VaR 95)'
plot filename using "ROW":5  title sprintf("HRP %0.3f\%", HRP_max) with lines, \
     filename using "ROW":6  title sprintf("MV %0.3f\%", MV_max) with lines, \
     filename using "ROW":3  title sprintf("1/N %0.3f\%", OON_max) with lines

set title "Instrument Weights of the Hierarchical Risk Portfolio"
plot \
  for [i=17:(16+weights):1] \
    filename using 1:(sum [col=i:(16+weights)] column(col)) \
      title columnheader(i) \
      with fillsteps fs solid

set title "Instrument Weights of the Minimum Variance Risk Portfolio"
plot \
  for [i=26:(25+weights):1] \
    filename using 1:(sum [col=i:(25+weights)] column(col)) \
      title columnheader(i) \
      with fillsteps fs solid