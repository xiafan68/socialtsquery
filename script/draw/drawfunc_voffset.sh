#!/bin/bash

#woffsets=("0" "24" "168" "336" "720")
widths=(1 4 6 12 24 `expr 24 \* 7` `expr 24 \* 30`)
#topk的值
ks=(10 20 50 100 150 200 350 400)

title=('invindex' 'minvindex')
idir=$1
odir=$2
odir="${odir}/offset"
if [ ! -d "${odir}" ]; then
echo "mkdir ${odir}"
mkdir -p "${odir}"
fi

for k in ${ks[@]}
do
for width in ${widths[@]}
do
ofile=$odir"/time_varing_offset_width${width}_k${k}.eps"
width=`expr ${width} \* 2`
ifile=$idir"offset/voffset_width_${width}_k_${k}.txt"
width=`expr ${width} / 2`
gnuplot<<EOF
#different type of approaches under fixed offset and width with varing ks
set terminal postscript eps color enhanced "Times-Roman" 20
set output "${ofile}"
set title "width ${width} h and top ${k}"
set ylabel "latency(ms)" font "Times-Roman,28"
set xlabel "Hours from abrupt time" font "Times-Roman,28"
plot "${ifile}" using (\$1/2):2 title 'invindex' with linespoints,'' using (\$1/2):3 title 'partitioned invindex' with linespoints,
EOF
done
done
