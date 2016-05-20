package org.openchai.spark.util

import java.net.InetAddress

/**
 * TcpUtils
 *
 */
object TcpUtils {
  @inline def getLocalHostname() = InetAddress.getLocalHost.getHostName

}
