#!/bin/sh

parts=(4 8 12 16 20)
prefix="/home/xiafan/expr/index/weibo/lsmo"
dataDir="/home/xiafan/data/weibo/sortedsegs"
#for test
#prefix="/Users/kc/Documents/temp/test/data"
#dataDir="/Users/kc/Documents/temp/test/sample"
for i in "${parts[@]}"
do
    to=`expr $i + 4`
    java -cp socialtsquery-1.0-jar-with-dependencies.jar util.IndexLoader -l conf/log4j-server2.properties -c conf/weibo/lsmo_scale/index_lsmo_weibo_part$i.conf -d ${dataDir}/part$i -e ./update_part$i.txt
    
    sleep 1000

    if [ $i -ne 20 ] ; then
	echo "copy from $i to " `expr $i + 4`
	cp ${prefix}/lsmo_weibo_part$i ${prefix}/lsmo_weibo_part`expr $i + 4`
	echo "copy completes"
    fi
done
