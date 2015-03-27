offsets=(0 2 12 24 48 `expr 24 \* 14` `expr 48 \* 30`)
widths=(2 8 12 24 48 `expr 24 \* 14` `expr 48 \* 30`)
actWidths=()
for ((i = 0; i < ${#widths[@]};i++))
do
    actWidths[i]=`expr ${widths[i]} / 2`
done
echo ${actWidths[@]}


actOffsets=() 
for ((i = 0; i < ${#offsets[@]};i++))
do
    actOffsets[i]=`expr ${offsets[i]} / 2`
done
echo ${actOffsets[@]}

ks=(10 20 50 100 150 200 250 300 350 400)