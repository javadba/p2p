package org.openchai.spark.p2p

/**
 * P2pServer
 *
 */
trait P2pServer {
  def start(): Boolean
  def stop(): Boolean
  def run(): Unit
}
