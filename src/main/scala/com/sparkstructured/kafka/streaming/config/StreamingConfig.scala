package com.sparkstructured.kafka.streaming.config

import com.sparkstructured.kafka.streaming.constant.StreamingConstant

import scala.beans.BeanProperty

class Conf extends Serializable {
  @BeanProperty var key: String = _
  @BeanProperty var value: String = _

}

class SparkStreamingConfig extends Serializable {
  @BeanProperty var sparkStreaming: java.util.List[String] = _
  @BeanProperty var appName: String = _
  @BeanProperty var master: String = _
  @BeanProperty var streamingBatchInterval: Int = StreamingConstant.STREAMING_BATCH_INTERVAL
  @BeanProperty var logLevel: String = StreamingConstant.ERROR
  @BeanProperty var isHiveSupportEnable: Boolean = true
  @BeanProperty var hadoopUser: String = _
  @BeanProperty var conf: java.util.List[Conf] = _
}

class SparkConfig extends Serializable {
  @BeanProperty var spark: java.util.List[String] = _
  @BeanProperty var appName: String = _
  @BeanProperty var master: String = _
  @BeanProperty var logLevel: String = StreamingConstant.ERROR
  @BeanProperty var conf: java.util.List[Conf] = _
}

class KafkaConfig extends Serializable {
  @BeanProperty var bootstrapServers: String = _
  @BeanProperty var schemaRegistryUrl: String = _
  @BeanProperty var saslJaasConfig: String = _
  @BeanProperty var options: java.util.List[Conf] = _
}

class StreamingConfig extends Serializable {
  @BeanProperty var sparkStreaming: SparkStreamingConfig = _
  @BeanProperty var spark: SparkConfig = _
  @BeanProperty var kafka: KafkaConfig = _
}
