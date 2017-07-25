# socialtsquery

## core
This project implements indices supporting time travel keyword search over social media data. Currently three indexing structures, i.e., LSMO, TPII and LSMI are supported. The query processing algorithm for processing the top-k temporal keyword over social media data is also implemented based on those indices.

## standalone server
Thrift is used to implement a RPC server so that users can use thrift client to remotely invoke insertion and query operations.
