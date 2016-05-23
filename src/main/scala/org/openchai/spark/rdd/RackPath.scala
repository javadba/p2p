/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.openchai.spark.rdd

case class RackPath(host: String, rack: String, path: String) {
  val fullPath = RackPath.toFullPath(host, rack, path)
}

object RackPathTester {
  def hostToRack(host: String) = {
    s"rack${hostToPartitionIndex(host)}"
  }

  def hostToPartitionIndex(host: String) = {
    host match {
      case h if h <= "d" => 1
      case h if h <= "l" => 2
      case _ => 3
    }
  }

  def hostsInRack(hostnames: Seq[String], rack: String) = {

  }
}

object RackPath {
  def toFullPath(host: String, rack: String, path: String) = s"$host:$rack:$path"

  def apply(fullPath: String) =
    new RackPath(
      fullPath.substring(0, fullPath.indexOf(':')),
      fullPath.substring(fullPath.indexOf(':') + 1, fullPath.lastIndexOf(':')),
      fullPath.substring(fullPath.lastIndexOf(':')+1))

  def hostToRack(host: String) = RackPathTester.hostToRack(host)

  def rackPathsToPartitions[T](rackPaths: Seq[RackPath]) =
    rackPaths.zipWithIndex.map { case (rp, x) => LsRDDPartition[T](x, rp) }

  implicit def strToRackPath(path: String): RackPath = apply(path)
}

