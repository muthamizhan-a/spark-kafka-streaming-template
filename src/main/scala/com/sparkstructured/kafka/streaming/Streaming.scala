package com.sparkstructured.kafka.streaming

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.io.Source
import com.sparkstructured.kafka.streaming.config.StreamingConfig
import com.sparkstructured.kafka.streaming.constant.StreamingConstant
import com.sparkstructured.kafka.streaming.exception.CustomException
import com.sparkstructured.kafka.streaming.kafka.KafkaContextsHandler
import com.sparkstructured.kafka.streaming.spark.SparkContextsHandler

class Streaming(projectName: Option[String] = None, streamingConfigFileName: Option[String] = None) {

  var kafka: KafkaContextsHandler = _
  var spark: SparkContextsHandler = _

  private val configSource = Source.fromURL(
    s"${StreamingConstant.CONFIG_MGMT_BASE_PATH}${projectName.getOrElse(StreamingConstant.DEFAULT_CONFIG_PROJECT)}/${streamingConfigFileName
      .getOrElse(StreamingConstant.DEFAULT_CONFIG_FILE)}.yaml"
  )
  private val streamingConf =
    new Yaml(new Constructor(classOf[StreamingConfig])).load(configSource.mkString).asInstanceOf[StreamingConfig]

  spark = new SparkContextsHandler(
    streamingConf.sparkStreaming.master,
    streamingConf.sparkStreaming.appName,
    streamingConf.sparkStreaming.conf,
    streamingConf.sparkStreaming.logLevel,
    streamingConf.sparkStreaming.streamingBatchInterval,
    streamingConf.sparkStreaming.isHiveSupportEnable,
    streamingConf.sparkStreaming.hadoopUser
  )
  kafka = new KafkaContextsHandler(streamingConf.kafka, spark)
  if (Option(spark).isEmpty) {
    throw new CustomException("Spark is not initialized due to which Kafka initialization failed.")
  }

}
