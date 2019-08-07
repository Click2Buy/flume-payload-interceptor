# Flume Payload Interceptor

## Build
mvn package

## Install
Copy target/flume-payload-interceptor-1.0.0.jar and janino-2.5.16.jar in Flume' classpath

## Configure

## Example 1

hbaseagent.sources.avro.interceptors = hbase_key
hbaseagent.sources.avro.interceptors.hbase_key.type = com.marketconnect.flume.interceptor.PayloadInterceptor$Builder
hbaseagent.sources.avro.interceptors.hbase_key.payload = header_1 + "-" + header_2
hbaseagent.sources.avro.interceptors.hbase_key.output = event_body

It will use janino to concat the value of header_1 with the constant - and with the value of header_2. And then put the result in the body of the event.

## Example 2

hbaseagent.sources.avro.interceptors = hbase_key
hbaseagent.sources.avro.interceptors.hbase_key.type = com.marketconnect.flume.interceptor.PayloadInterceptor$Builder
hbaseagent.sources.avro.interceptors.hbase_key.payload = ((Integer.parseInt(header_1) > 0) ? "nothing" : "something")
hbaseagent.sources.avro.interceptors.hbase_key.output = header_2

It will use janino to put nothing or something depneding on the value of header_1 in header header_2.

The result MUST be a String.