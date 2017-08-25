#!/bin/bash

parts=(4 8 12 16 20)
prefix="/home/xiafan/expr/index/weibo/octree"
dataDir="/home/xiafan/data/weibo/sortedsegs"

for i in "${parts[@]}"
do
    to=`expr $i + 4`
    echo $to
    java -Xms5000M -Xmx5000M -cp target/socialtsquery-1.0-jar-with-dependencies.jar util.IndexLoader -l conf/log4j.properties -c conf/weibo/octree_scale/index_octree_weibo_part$i.conf -d ${dataDir}/part$i -e ./update_octree_part$i.txt
    
    sleep 10

    if [ $i -ne 20 ] ; then
	echo "copy from $i to " `expr $i + 4`
	cp -r ${prefix}/octree_weibo_part$i ${prefix}/octree_weibo_part`expr $i + 4`
	echo "copy completes"
    fi
done
