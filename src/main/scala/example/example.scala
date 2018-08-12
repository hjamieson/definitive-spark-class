package example

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

import collection.JavaConverters._

/**
  * Description:
  *     reads some data on HDFS and imports it into a table in HBase.
  *
  * Args:
  *     <input-file> a JSON flle to read.
  *     <table> the hbase table to store the data into.
  */
object ImportData {
    def main(args: Array[String]): Unit = {
        require(args.length >1, "args: <input-json(hdfs)> <target-table(hbase)>")
        val table = TableName.valueOf(args(1))
        val inputJson = args(0)

        // initialize the spark engine
        val sparkConf = new SparkConf().setAppName("HBase Table Import")
        val sc = new SparkContext(sparkConf)

        // load the data
        val om = new ObjectMapper()
        val rdd = sc.textFile(inputJson).map((j: String) => om.readValue(j, classOf[java.util.Map[String, String]]))
//        rdd.foreach(l => println(l))

        // push the data to HBase
        val hbc = HBaseConfiguration.create()
        new HBaseContext()

    }
}