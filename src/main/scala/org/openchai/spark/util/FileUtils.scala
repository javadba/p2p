package org.openchai.spark.util

import java.io.File
import java.nio.file.Paths
import java.util.Scanner
import java.util.concurrent.{CompletableFuture, Callable, Executors}

import java.util.concurrent.Future

object FileUtils {

  import Logger._

  def readPath(path: String, recursive: Boolean = true, multiThreaded: Boolean = true): String = {
    val nThreads = if (multiThreaded) {
      Runtime.getRuntime.availableProcessors * 2
    } else {
      1
    }
    val tpool = Executors.newFixedThreadPool(nThreads)
    class ReadTask(path: String) extends Callable[String] {
      override def call(): String = {
        readFile(path)
      }
    }
    val sb = new StringBuffer // Make sure StringBUFFER not BUILDER because of multithreaded!!

    import collection.mutable
    val tasksBuf = mutable.ArrayBuffer[Future[String]]()
    def readPath0(fpath: String): String = {
      val paths = new File(fpath).listFiles.filter { f => !f.getName.startsWith(".") }
      paths.foreach { f =>
        if (f.isDirectory) {
          if (recursive) {
            debug(s"Descending into ${f.getPath} ..")
            readPath0(f.getPath)
          } else {
            debug(s"Recursive is false so NOT descending into ${f.getPath}")
          }
        } else {
          tasksBuf += tpool.submit(new ReadTask(f.getPath))
        }
      }
      tasksBuf.foreach { t => sb.append(t.get) }
      sb.toString
    }
    readPath0(path)
  }

  def readFile(fpath: String) = {
    val content = new Scanner(Paths.get(fpath)).useDelimiter("\\Z").next()
    content
  }
}
