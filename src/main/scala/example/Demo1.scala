package example

import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {

  def main(args: Array[String]): Unit = {
    val scf = new SparkConf().setAppName("demo1")
    val sc = new SparkContext(scf)
    val sample = sc.parallelize(1 to 1000)
    val evens = sample.filter(_ % 2 == 0)
    try {
      evens.saveAsTextFile("target/evens.out")
    } catch {
      case e: Exception => e.printStackTrace()
    }

    sc.stop()
  }


}
