set terminal jpeg medium size 16400,480
#set terminal pngcairo font "arial,10" size 500,500
if (!exists("filename")) filename='/tmp/db.4.csv'
#set output 'candlestick.png'
#set xdata time
#set timefmt"%Y-%m-%d"
#set xrange ["2013-02-14":"2013-02-27"]
set yrange [*:*]
set datafile separator ","
plot filename using 1:2:4:3:5 notitle with candlesticks