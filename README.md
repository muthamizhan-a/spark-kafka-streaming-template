# spark-kafka-streaming-template
### A generic spark structured streaming + kafka streaming utility library with integration hive.

## Run locally

### start docker containers
cd docker-local-setup



## code sample

```
package com.sparkstructured.kafka

import com.sparkstructured.kafka.streaming.Streaming

object AppMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val streaming = new Streaming()
    val dataframe = streaming.spark.sql("select * from db.tbl")
    dataframe.show()
  }
}
```
