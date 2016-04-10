#/bin/bash
#drawing the details of factor width
#setup all parameters

source setupparams.sh

#echo "ks ",${ks[@]}

genFigure(){
#number of rows puts in a cluster
    barNum=2
    
#colums to skip
    colSkip=3
#rows to skip
    rowskip_arg=2
    faLen=${#clusterAxis[@]}
    compsLen=${#comps[@]}
    xLen=${#xaxis[@]}
    #echo $ifile
    echo "start to generate figures for $cName $type"
    si=0
    for (( si = 0; si < $faLen; si++))
    do
        plot="plot "
        rowskip=$rowskip_arg
        for (( i = 0; i < ${xLen}; i++))
        do
            end=`expr $rowskip + $barNum - 1`
            plot=$plot"newhistogram '${xaxis[$i]}' lt 1"
	    #echo  $colSkip
            if [ $i -eq '0' ]; then
                for (( j =0; j < ${compsLen}; j++))
                do
                    plot=$plot",'$ifile' every ::$rowskip::$end using `expr $colSkip + $j`:xtic(2) title '${comps[$j]}'"
                done
            else
                for (( j =0; j < ${compsLen}; j++))
                do
                    plot=$plot",'$ifile' every ::$rowskip::$end using `expr $colSkip + $j`:xtic(2) notitle"
                done
            fi

            if [ $i -ne `expr $xLen - 1` ];
            then
                plot=$plot","
            fi

            rowskip=`expr $end + 1`
            #echo "iteration $i $colSkip"
        done
	#echo $plot
        gnuplot<<EOF
set terminal postscript eps color enhanced "Times-Roman" 20
set output "$odir/varing_k_${cName}${clusterAxis[$si]}_${dim3Name}_${dim3}_${type}.eps"
#set title "${title}"
set ylabel "$ylab" font "Times-Roman,28"
set xlabel "$xlab" font "Times-Roman,28" offset 0,-2
set xtics nomirror rotate by 90 offset 0,-1.5
set key ${legPos}
set style data histogram
set style histogram rowstacked title offset 0,0.5

$plot
#########################################################
EOF
        colSkip=`expr $colSkip + $compsLen`
    done
}

function setupIO(){
    comps=('atomic')
    type="io"
    ylab="# of Blocks(1KB)"
    #title="IO Costs of Different Parts"
}

function setupTime(){
    comps=('update' 'atomic\_time')
    type="time"
    ylab="Time Elapse(ms)"
    #title="Time Costs of Different Parts"
}

function setupK(){
    #clusterAxis=(${widths[@]})
    clusterAxis=(${actWidths[@]})
    xaxis=(${ks[@]})
    cName="width"
    dim3Name="offset"
    odir="${odirP}/k_${type}/"
    xlab="TopK"
    if [ ! -d ${odir} ]; then
	mkdir -p $odir
    fi

    ifile="${idir}/k_${type}/k_${cName}_s${dim3}_${type}.txt"
    legPos="left top"
}

function setupWidth(){
    clusterAxis=(${ks[@]})
    #xaxis=(${widths[@]}) 
    xaxis=(${actWidths[@]})
    cName="k"
    dim3Name="offset"
    odir="${odirP}/w_${type}/"
    if [ ! -d ${odir} ]; then
	mkdir -p $odir
    fi
    xlab="Width of Query Time Window"
    ifile="${idir}/w_${type}/width_${cName}_s${dim3}_${type}.txt"
    legPos="right top"
}

function setupOffset(){
    clusterAxis=(${ks[@]})
    #xaxis=(${widths[@]}) 
    xaxis=(${actOffsets[@]})
    cName="k"
    dim3Name="width"
    odir="${odirP}/offset_${type}/"
    if [ ! -d ${odir} ]; then
	mkdir -p $odir
    fi
    xlab="Time shift from Abrupt Time of Events(Hours)"
    ifile="${idir}/offset_${type}/offset_${cName}_${dim3Name}_${dim3}_${type}.txt"
    legPos="left top"
}

function setupMem(){
    clusterAxis=(0)
    #xaxis=(${widths[@]}) 
    xaxis=(1 2 3 4)
    cName="none"
    dim3Name="none"
    odir="${odirP}/mem_${type}/"
    if [ ! -d ${odir} ]; then
	mkdir -p $odir
    fi
    xlab="Memory Size(GB)"
    ifile="${idir}/throughput_${type}.txt"
    legPos="right top"
}


idir=$1
odirP=$2

drawtime="2015_01_18"

#setupTime
#setupMem
#genFigure
#setupIO
#setupMem
#genFigure

#exit
for dim3 in ${widths[@]}
do
    setupTime
    setupOffset
    genFigure

    setupIO
    setupOffset
    genFigure
done

odir="${odirP}"
./drawfunc_vk_time.sh $idir $odir

odir="${odirP}"
./drawfunc_vw_time.sh $idir $odir
./drawfunc_voffset.sh $idir $odir

for dim3 in ${offsets[@]}
do
setupTime

setupWidth
genFigure

setupK
genFigure

setupIO

setupWidth
genFigure

setupK
genFigure
done
