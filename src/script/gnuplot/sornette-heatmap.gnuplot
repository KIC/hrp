if (!exists("outterm")) outterm='jpeg'
set terminal outterm size 1700,768
# set terminal wx size 700,300
set boxwidth 1 relative
set palette defined (0 "white", 1 "red")
set y2tics
set datafile separator ','
stat filename using 3 skip 1 nooutput
set cbrange [0:(STATS_max)]
set y2range [0:1]

set style fill solid
plot filename using 1:(1):3 with boxes linecolor palette axes x1y2, \
     filename using 1:2 with lines lc rgb "#000000" axes x1y1

