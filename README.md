# Flume Payload Interceptor

## Build
mvn package

## Install
Copy target/flume-payload-interceptor-1.0.0.jar in Flume' classpath

## Configure
hbaseagent.sources.avro.interceptors = hbase_key
hbaseagent.sources.avro.interceptors.hbase_key.type = com.marketconnect.flume.interceptor.PayloadInterceptor$Builder
hbaseagent.sources.avro.interceptors.hbase_key.payload = [header_1] + "-" + [header_2]

It will concat the value of header_1 with the constant - and with the value of header_2