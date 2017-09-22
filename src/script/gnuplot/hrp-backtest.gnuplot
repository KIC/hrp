set terminal jpeg small size 1024,1536
#if (!exists("filename")) filename='/tmp/db.4.csv'

set xdata time
set timefmt"%s"
set datafile separator ","
set multiplot layout 3,1 rowsfirst
set title "Performance"
plot filename using 1:2 title '1/N' with lines, \
     filename using 1:5 title 'HRP' with lines
set title "Daily Value at Risk (VaR 95)"
plot filename using 1:4 title '1/N' with lines, \
     filename using 1:7 title 'HRP' with lines
set title "Instrument Weights of the Hierarchical Risk Portfolio"
plot \
  for [i=8:weights:1] \
    filename using 1:(sum [col=i:weights] column(col)) \
      title columnheader(i) \
      with fillsteps fs solid