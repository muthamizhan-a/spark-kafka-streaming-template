package com.sparkstructured.kafka.streaming.kafka

import java.util

import com.spark.streaming.config.KafkaConfig
import com.spark.streaming.spark.SparkContextsHandler
import com.sparkstructured.kafka.streaming.config.KafkaConfig
import com.sparkstructured.kafka.streaming.exception.CustomException
import com.sparkstructured.kafka.streaming.spark.SparkContextsHandler
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

/**
  * Handler communications with Kafka and schema registry
  *
  * @param kafkaConfig          : kafka config
  * @param sparkContextsHandler : Spark contexts handler
  */
class KafkaContextsHandler(kafkaConfig: KafkaConfig, sparkContextsHandler: SparkContextsHandler) extends LazyLogging {
  if (Option(sparkContextsHandler).isEmpty) {
    throw new CustomException("Spark is not initialized due to which Kafka initialization failed.")
  }

  private val consumerSchemaRegistoryConfs: HashMap[String, Map[String, String]] =
    new HashMap[String, Map[String, String]]
  private val producerSchemaRegistoryConfs: HashMap[String, Map[String, String]] =
    new HashMap[String, Map[String, String]]()
  private val kafkaOptions: java.util.Map[String, String] = new util.HashMap[String, String]()
  for (option <- kafkaConfig.options.asScala) {
    kafkaOptions.put(option.key, option.value)
  }

  protected def readFromKafka(kafkaBootstrapServers: String, kafkaTopic: String): DataFrame = {
    sparkContextsHandler.sparkSession.get.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .options(kafkaOptions)
      .load()
  }

  def streamFromConfluentAsAvro(topic: String): DataFrame = {
    readFromKafka(kafkaConfig.bootstrapServers, topic)
      .select(from_confluent_avro(col("value"), getConsumerSchemaRegistoryConf(topic)) as 'data)
      .select("data.*")
  }

  def streamToConfluentAsAvro(
      dataFrame: DataFrame,
      topic: String,
      schemaName: String,
      schemaNameSpace: String
  ): DataStreamWriter[Row] = {
    dataFrame
      .select(
        to_confluent_avro(
          struct(dataFrame.columns.head, dataFrame.columns.tail: _*),
          getProducerSchemaRegistoryConf(topic, schemaName, schemaNameSpace)
        ) as 'value
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("topic", topic)
      .options(kafkaOptions)
  }

  private def getConsumerSchemaRegistoryConf(topic: String): Map[String, String] = {
    var schemaRegistryConf = consumerSchemaRegistoryConfs.getOrElse(topic, null)
    if (schemaRegistryConf == null) {
      schemaRegistryConf = Map(
        SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> kafkaConfig.schemaRegistryUrl,
        SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
        SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
        SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
      )
      consumerSchemaRegistoryConfs.put(topic, schemaRegistryConf)
    }
    schemaRegistryConf
  }

  private def getProducerSchemaRegistoryConf(
      topic: String,
      schemaName: String,
      schemaNameSpace: String
  ): Map[String, String] = {
    var schemaRegistryConf = producerSchemaRegistoryConfs.getOrElse(topic, null)
    if (schemaRegistryConf == null) {
      schemaRegistryConf = Map(
        SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> kafkaConfig.schemaRegistryUrl,
        SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
        SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
        SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> schemaName,
        SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> schemaNameSpace
      )
      producerSchemaRegistoryConfs.put(topic, schemaRegistryConf)
    }
    schemaRegistryConf
  }
}
