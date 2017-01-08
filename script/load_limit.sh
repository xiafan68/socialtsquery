#!/bin/bash

tao=(1 5 10 15)
prefix="/home/xiafan/expr/index/weibo/lsmi"
dataDir="/home/xiafan/data/weibo/sortedsegs"
#for test
#prefix="/Users/kc/Documents/temp/test/data"
#dataDir="/Users/kc/Documents/temp/test/sample"
for i in "${tao[@]}"
do
    java -Xms5000M -Xmx5000M -cp socialtsquery-1.0-jar-with-dependencies.jar util.IndexLoader -l conf/log4j-server2.properties -c conf/weibo/tao/index_lsmi_weibo$i.conf -d ${dataDir}/part4 -e ./update_lsmi_part4.txt
     java -Xms5000M -Xmx5000M -cp socialtsquery-1.0-jar-with-dependencies.jar util.IndexLoader -l conf/log4j-server2.properties -c conf/weibo/tao/index_lsmi_weibo$i.conf -d ${dataDir}/part8 -e ./update_lsmi_part8.txt
     java -Xms5000M -Xmx5000M -cp socialtsquery-1.0-jar-with-dependencies.jar util.IndexLoader -l conf/log4j-server2.properties -c conf/weibo/tao/index_lsmi_weibo$i.conf -d ${dataDir}/part12 -e ./update_lsmi_part12.txt
     java -Xms5000M -Xmx5000M -cp socialtsquery-1.0-jar-with-dependencies.jar util.IndexLoader -l conf/log4j-server2.properties -c conf/weibo/tao/index_lsmi_weibo$i.conf -d ${dataDir}/part16 -e ./update_lsmi_part16.txt
    
    sleep 10
done
