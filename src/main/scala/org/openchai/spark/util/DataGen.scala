package org.openchai.spark.util
object DataGen {
  def main(args: Array[String]) = {
    import java.util.Random
    val dir = "/data/lsrdd"
    import java.io._
    val f = new File(dir)
    if (!f.exists) f.mkdirs
    val dirs = (0 until 10).map( ix => s"$dir/$ix/lsdata")
    dirs.foreach{d => 
      val f = new File(d)
      if (!f.exists) f.mkdirs
    }
    val files = dirs.map( p => List(s"$p.1",s"$p.2",s"$p.3")).flatten
      files.foreach{ f => 
        val r = new Random
        val Base = 10.0
        val ArrSize = 10
        val NRecs = 300 
        val nrecs = NRecs
        def round(d: Double, k: Int) = (d * Math.pow(10,k)).toLong / Math.pow(10,k)
        val sb = (0 until nrecs).foldLeft(new StringBuffer){ case (sb, ix) =>
          sb.append(s"$ix\t${round(Base * r.nextDouble,2)}")
          val vstr = (0 until ArrSize).foldLeft(new StringBuffer){ case (sbb,xx) =>
            sbb.append(s"\t${round(Base * r.nextDouble,2)}")
          }
         sb.append(vstr + "\n")
        }
        writeFile(f, sb.toString)
      }
  }
  def writeFile(path: String, str: String) = {
    tools.nsc.io.File(path).writeAll(str)
  }
}
