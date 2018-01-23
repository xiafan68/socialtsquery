# socialtsquery

## core
This project implements indices supporting time travel keyword search over social media data. Currently three indexing structures, i.e., LSMO, TPII and LSMI are supported. The query processing algorithm for processing the top-k temporal keyword over social media data is also implemented based on those indices.

## standalone server
Thrift is used to implement a RPC server so that users can use thrift client to remotely invoke insertion and query operations.


## HowTo
1. execute the following command to package the project into a  Jar file. It will generate the file socialtsquery-1.0-jar-with-dependencies.jar in directory target.
```
mvn assembly:assembly -Dmaven.test.skip=true
```

2.  Examine scripts prefixed with load in directory "script" to find out how we can use the generated jar to load data.

3. execute the following commands to run tests:
```
java -Xmx 5024M -jar  target/socialtsquery-1.0-jar-with-dependencies.jar expr.DiskBasedPerfTest  -c conf/weibo/lsmo_bdb_scale/index_lsmo_bdb_weibo_part4.conf  -c conf/weibo/lsmo_bdb_scale/index_lsmo_bdb_weibo_part8.conf  -c conf/weibo/lsmo_bdb_scale/index_lsmo_bdb_weibo_part12.conf -c conf/weibo/lsmo_bdb_scale/index_lsmo_bdb_weibo_part16.conf  -c conf/weibo/lsmo_bdb_scale/index_lsmo_bdb_weibo_part20.conf -e /home/xiafan/expr/keywordnum_2_50/weibo -q /home/xiafan/Dropbox/数据/query/weibo/nqueryseed.txt -s facts
```