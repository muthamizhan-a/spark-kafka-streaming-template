package com.sparkstructured.kafka.streaming.spark

import java.sql.DriverManager
import java.util.Date

import com.sparkstructured.kafka.streaming.config.Conf
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Spark context command pattern: It is describing all behavior of all spark contexts
  *
  * @param master                          : spark master url
  * @param applicationName                 : application name
  * @param options                         : spark configuration
  * @param logLevel                        : spark log level. By default is DEBUG
  * @param streamingBatchIntervalInSeconds : Spark stream batch duration. By default is 1 minute
  */
class SparkContextsHandler(
    master: String,
    applicationName: String,
    options: java.util.List[Conf],
    logLevel: String,
    streamingBatchIntervalInSeconds: Long,
    isHiveSupportEnable: Boolean,
    hadoopUser: String
) extends LazyLogging {

  private val _sparkContextsFactory: SparkContextsFactory = new SparkContextsFactory()
  private val _sparkConf = Try(
    _sparkContextsFactory.configBuilder(Some(master), Some(applicationName), options, Some(hadoopUser))
  ).get
  private val _sparkSession = Try(
    _sparkContextsFactory.sparkSessionBuilder(_sparkConf, Some(logLevel), isHiveSupportEnable)
  ).get
  private val _sparkStreamContext = Try(
    _sparkContextsFactory.streamingContextBuilder(sparkSession, Some(streamingBatchIntervalInSeconds))
  ).get
  private val _sparkContext = Try(_sparkContextsFactory.sparkContextBuilder(sparkSession)).get

  def sparkSession: Option[SparkSession] = _sparkSession

  def sparkStreamingContext: Option[StreamingContext] = _sparkStreamContext

  def sparkContext: Option[SparkContext] = _sparkContext

  protected def startStreaming() {
    _sparkStreamContext.get.start()
    _sparkStreamContext.get.awaitTermination()
  }

  def stopSpark() {
    _sparkSession.get.stop()
  }

  def saveToHive(
      dataFrame: DataFrame,
      table: String,
      partitionColumn: String,
      saveMode: SaveMode = SaveMode.Append
  ): Unit = {
    if (_sparkSession == null) {
      dataFrame.write.partitionBy(partitionColumn).mode(saveMode).format("orc").saveAsTable(table)
    } else {
      logger.error("No spark context was initialized .... ")
    }
  }

  /**
    * Stores streaming dataframe into provided datasource (Postgres, mysql, etc)
    *
    * @param dataFrame
    * @param jdbc_url
    * @param jdbcDriver
    * @param jdbcDatabaseName
    * @param resultTable
    * @param rdsColumnOrder
    * @return
    */
  def writeToJdbcSink(
      dataFrame: DataFrame,
      jdbc_url: String,
      jdbcDriver: String,
      jdbcDatabaseName: String,
      resultTable: String,
      rdsColumnOrder: String
  ): DataStreamWriter[Row] = {
    dataFrame.writeStream.foreach(new ForeachWriter[Row] {
      var connection: java.sql.Connection = _
      var statement: java.sql.Statement = _
      val noOfCols: Int = rdsColumnOrder.split(",").length - 1

      def open(partitionId: Long, version: Long): Boolean = {
        Class.forName(jdbcDriver)
        connection = DriverManager.getConnection(jdbc_url)
        statement = connection.createStatement

        true
      }

      def process(values: Row): Unit = {
        val valueBuilder = new StringBuilder
        (0 to noOfCols).foreach(pos =>
          values(pos) match {
            case s: String        => valueBuilder.append("'" + s + "',")
            case c: Char          => valueBuilder.append("'" + c + "',")
            case d: Date          => valueBuilder.append("'" + d + "',")
            case t: TimestampType => valueBuilder.append("'" + t.formatted("yyyy-MM-dd HH:mm:ss") + "',")
            case o                => valueBuilder.append(o + ",")
          }
        )
        val valueStr = StringUtils.substring(valueBuilder.mkString, 0, -1)
        statement.execute(
          "INSERT INTO " + jdbcDatabaseName + "." + resultTable + " (" + rdsColumnOrder + ") VALUES (" + valueStr + ")"
        )
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close()
      }
    })
  }

  def sql(query: String): DataFrame = {
    _sparkSession.get.sqlContext.sql(query)
  }

  def sqlContext(): SQLContext = {
    _sparkSession.get.sqlContext
  }
}

/**
  * Spark contexts factory.
  * It creates : SparkContext, SparkStreamingContext, SparkSession and of course SparkConf.
  * It is only accessible only from this package or from unit tests
  */
protected class SparkContextsFactory() extends LazyLogging {
  private val DEFAULT_LOG_LEVEL = "INFO"
  private val DEFAULT_SPARK_MASTER = "local[*]"
  private val DEFAULT_APP_NAME = "local"
  private val DEFAULT_HADOOP_USER = System.getProperty("HADOOP_USER_NAME")
  private val DEFAULT_STREAMING_BATCH_INTERVAL_IN_SECONDS = 60000

  def configBuilder(
      master: Option[String],
      appName: Option[String],
      options: java.util.List[Conf],
      hadoopUser: Option[String]
  ): Option[SparkConf] = {
    System.setProperty("HADOOP_USER_NAME", hadoopUser.getOrElse(DEFAULT_HADOOP_USER))
    val sparkConf =
      new SparkConf()
        .setMaster(master.getOrElse(DEFAULT_SPARK_MASTER))
        .setAppName(appName.getOrElse(DEFAULT_APP_NAME))
    for (conf <- options.asScala) {
      sparkConf.set(conf.key, conf.value)
    }
    Some(sparkConf)
  }

  /**
    * Handler initializing spark session
    *
    * @param sparkConf
    * @param logLevel
    * @return
    */
  def sparkSessionBuilder(
      sparkConf: Option[SparkConf],
      logLevel: Option[String],
      isHiveSupportEnable: Boolean
  ): Option[SparkSession] = {
    sparkConf match {
      case `sparkConf` =>
        var sparkSession: SparkSession = null
        if (isHiveSupportEnable) {
          sparkSession = SparkSession
            .builder()
            .config(sparkConf.get)
            .enableHiveSupport()
            .getOrCreate()
        } else {
          sparkSession = SparkSession
            .builder()
            .config(sparkConf.get)
            .getOrCreate()
        }

        sparkSession.sparkContext.setLogLevel(logLevel.getOrElse(DEFAULT_LOG_LEVEL))
        Some(sparkSession)
      case None => throw new IllegalArgumentException("SparkConf is null!")
    }
  }

  def sparkContextBuilder(sparkSession: Option[SparkSession]): Option[SparkContext] = {
    sparkSession match {
      case `sparkSession` => Some(sparkSession.get.sparkContext)
      case None           => throw new IllegalArgumentException("sparkSession is null!")
    }
  }

  def streamingContextBuilder(
      sparkSession: Option[SparkSession],
      streamingBatchIntervalInSeconds: Option[Long]
  ): Option[StreamingContext] = {
    sparkSession match {
      case `sparkSession` =>
        logger.info("Initializing spark streaming context...")
        val sparkStreamingContext = new StreamingContext(
          sparkSession.get.sparkContext,
          Seconds(streamingBatchIntervalInSeconds.getOrElse(DEFAULT_STREAMING_BATCH_INTERVAL_IN_SECONDS))
        )
        Some(sparkStreamingContext)
      case None => throw new IllegalArgumentException("sparkSession is null!")
    }
  }

}
