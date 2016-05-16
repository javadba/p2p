package org.openchai.spark.util

object Logger {
  def debug(msg: String) = println(s"Debug: $msg")
  def info(msg: String) = println(s"Info: $msg")
  def error(msg: String) = println(s"ERROR: $msg")
}
