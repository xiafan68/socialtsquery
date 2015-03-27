#!/bin/bash

echo "drawing vk"
woffsets=("0" 2 12 "24" 48 336 1440)
#widths=(1 2 3 4 5 12 24)
widths=(2 8 12 24 48 `expr 24 \* 14` `expr 48 \* 30`)
idir=$1
odir=$2
odir="${odir}/k/"
if [ ! -d "${odir}" ]; then
echo "mkdir ${odir}"
mkdir -p "${odir}"
fi

echo "odir is ${odir}"
for woffset in ${woffsets[@]}
do
i=0
for width in ${widths[@]}
do
ofile=$odir"time_varing_k_s${woffset}_w${width}.eps"
ifile=$idir"k_w/time/"
hpl_s_file=$ifile"invindex_o${woffset}w.txt"
hpl_ns_file=$ifile"minvindex_o${woffset}w.txt"

offset=`expr ${i} \* 3 + 2`
i=`expr ${i} + 1`
#echo "width ${width} offset ${woffset} iter ${i}"
gnuplot<<EOF
#different type of approaches under fixed offset and width with varing ks
set terminal postscript eps color enhanced "Times-Roman" 20
set output "${ofile}"
set title "${woffset} hours from start and query window of ${width} hours"
set ylabel "latency(ms)" font "Times-Roman,28"
set xlabel "Topk" font "Times-Roman,28"
plot "${hpl_s_file}" using 1:${offset} title 'invindex' with linespoints, "${hpl_ns_file}" using 1:${offset} title 'partitioned invindex' with linespoints

EOF
done
done
echo "drawing vk ends"