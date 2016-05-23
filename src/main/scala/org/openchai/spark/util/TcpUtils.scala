package org.openchai.spark.util

import java.net.InetAddress

object TcpUtils {
  @inline def getLocalHostname() = InetAddress.getLocalHost.getHostName

}
