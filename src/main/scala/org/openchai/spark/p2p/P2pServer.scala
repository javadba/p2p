package org.openchai.spark.p2p

trait P2pServer {
  def start(): Boolean
  def stop(): Boolean
  def run(): Unit
}
