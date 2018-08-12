package example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase._

object Demo2 {

  def main(args: Array[String]): Unit = {
    val scf = new SparkConf().setAppName("demo2")
    val sc = new SparkContext(scf)
    val hbc = org.apache.hadoop.hbase.HBaseConfiguration.create()

    try {
    } catch {
      case e: Exception => e.printStackTrace()
    }

    sc.stop()
  }


}
