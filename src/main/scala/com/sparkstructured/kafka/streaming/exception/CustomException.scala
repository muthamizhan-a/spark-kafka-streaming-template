package com.sparkstructured.kafka.streaming.exception

import com.typesafe.scalalogging.{LazyLogging}

class CustomException(msg: String) extends Exception(msg) with LazyLogging {

  def this(msg: String, cause: Throwable) = {
    this(msg)
    initCause(cause)
  }

  def this(cause: Throwable) = {
    this(Option(cause).map(_.toString).orNull)
    initCause(cause)
  }

  def this() = {
    this(null: String)
  }
}
