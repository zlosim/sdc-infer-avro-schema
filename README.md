# Streamsets infer avro schema processor
* based on [Streamsets`s schema processor](https://github.com/streamsets/datacollector)
* generates records instead of maps in avro schema
* generated avro schema should be the same that [nifi](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-kite-nar/1.7.1/org.apache.nifi.processors.kite.InferAvroSchema/index.html) with [kiteSDK](https://github.com/kite-sdk/kite/blob/1070ff9bc17b4db684e6a2962749a1dbb83d1d18/kite-data/kite-data-core/src/main/java/org/kitesdk/data/spi/JsonUtil.java#L539) generates