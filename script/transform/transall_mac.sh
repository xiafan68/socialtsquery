#!/bin/bash

idir=$1
odir=$2



offsets=(0 2 12 24 48 `expr 24 \* 14` `expr 48 \* 30`)
#offsets=(0 24)
tdate="2015_3_21"

python etoffset.py -p invindex minvindex -i $idir -s ${offsets[@]} -o "$odir/" -f total -x width -y k

python etoffset.py -i $idir -o $odir -k -t io -f atomic -s ${offsets[@]} -p invindex minvindex -x width -y k

python etoffset.py -i $idir -o $odir -k -t time -f update atomic_time -s ${offsets[@]}  -p invindex minvindex -x width -y k


for offset in ${offsets[@]}
do
python mergetransform.py -p invindex minvindex -i $idir -s $offset -o $odir"/k_io" -f atomic  -x k -y width -t io

python mergetransform.py  -p  invindex minvindex -i $idir -s $offset -o $odir"/k_time" -f total update atomic_time  -x k -y width -t time

python mergetransform.py -p  invindex minvindex -i $idir -s $offset -o $odir"/w_io" -f atomic -x width -y k -t io

python mergetransform.py  -p invindex minvindex -i $idir -s $offset -o $odir"/w_time" -f total update atomic_time -x width -y k -t time

python ./norm.py $idir/invindex_o${offset}w.txt $odir
python ./norm.py $idir/minvindex_o${offset}w.txt $odir
done