package org.openchai.spark.p2p
import java.net._

/**
 * TcpUtils
 *
 */
object TcpUtils {
  @inline def getLocalHostname() = InetAddress.getLocalHost.getHostName

}
