#!/bin/bash
#绘制在固定k,offset的前提下，不同的width对查询性能的影响
#查询偏移
woffsets=("0" 2 12 "24" 48 336 1440)
#查询窗口大小
widths=(2 8 12 24 48 `expr 24 \* 14` `expr 48 \* 30`)
#topk的值
ks=(10 20 50 100 150 200 350 400)

idir=$1
odir=$2
odir="${odir}/width"
if [ ! -d "${odir}" ]; then
echo "mkdir ${odir}"
mkdir -p "${odir}"
fi

#varing width
for woffset in ${woffsets[@]}
do
i=0
for k in ${ks[@]}
do
ofile=$odir"/time_varing_w_s${woffset}_k${k}.eps"
ifile=$idir"w_k/time/"
hpl_s_file=$ifile"invindex_o${woffset}w.txt"
hpl_ns_file=$ifile"minvindex_o${woffset}w.txt"

offset=`expr ${i} \* 3 + 2`
gnuplot<<EOF
#different type of approaches under fixed offset and width with varing width
set terminal postscript eps color enhanced "Times-Roman" 20
set output "${ofile}"
set title "$Time cost of Each Part"
set ylabel "latency(ms)" font "Times-Roman,28"
set xlabel "Query Width" font "Times-Roman,28"
plot "${hpl_s_file}" using (\$1/2):${offset} title 'invindex' with linespoints, "${hpl_ns_file}" using (\$1/2):${offset} title 'partitioned invindex' with linespoints
EOF

i=`expr ${i} + 1`
done
done
echo "drawing vw ends"