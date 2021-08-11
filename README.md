# spark-streaming-template

## code sample
package com.sparkstructured.kafka

import com.sparkstructured.kafka.streaming.Streaming

object AppMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    println("STARTING APPLICATION....")
    val streaming = new Streaming()
    val dataframe = streaming.spark.sql("select * from test_db.test_tbl")
    dataframe.show()
  }
}
