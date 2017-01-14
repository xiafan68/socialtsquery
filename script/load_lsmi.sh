#!/bin/bash

parts=(4 8 12 16 20)
prefix="/home/xiafan/expr/index/weibo/lsmi"
dataDir="/home/xiafan/data/weibo/sortedsegs"
#for test
#prefix="/Users/kc/Documents/temp/test/data"
#dataDir="/Users/kc/Documents/temp/test/sample"
for i in "${parts[@]}"
do
    to=`expr $i + 4`
    echo $to
    java -Xms5000M -Xmx5000M -cp target/socialtsquery-1.0-jar-with-dependencies.jar util.IndexLoader -l conf/log4j-server2.properties -c conf/weibo/lsmi_scale/index_lsmi_weibo_part$i.conf -d ${dataDir}/part$i -e ./update_lsmi_part$i.txt
    
    sleep 10

    if [ $i -ne 20 ] ; then
	echo "copy from $i to " `expr $i + 4`
	cp -r ${prefix}/lsmi_weibo_part$i ${prefix}/lsmi_weibo_part`expr $i + 4`
	echo "copy completes"
    fi
done
