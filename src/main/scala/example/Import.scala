package example

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

import collection.JavaConverters._
import scala.collection.mutable

/**
  * Description:
  * reads some data on HDFS and imports it into a table in HBase.
  *
  * Args:
  * <input-file> a JSON flle to read.
  * <table> the hbase table to store the data into.
  */
object ImportData {
  def main(args: Array[String]): Unit = {
    require(args.length > 1, "args: <input-json(hdfs)> <target-table(hbase)>")
    val table = TableName.valueOf(args(1))
    val inputJson = args(0)

    // initialize the spark engine
    val sparkConf = new SparkConf().setAppName("HBase Table Import")
    val sc = new SparkContext(sparkConf)

    // load the data
    val om = new ObjectMapper()
    val rdd = sc.textFile(inputJson).map(j => {
      val parts = j.split(raw"\t")
      val m = om.readValue(parts(1), classOf[java.util.Map[String, String]]).asScala
      m("key")= parts(0)
      m
    })


    // push the data to HBase
    rdd.foreachPartition { part =>
      val hbc = HBaseConfiguration.create()
      val conn = ConnectionFactory.createConnection(hbc)
      val htable = conn.getTable(table)

      part.foreach(rec => {
        val put = makePut(rec, "d".getBytes())
        htable.put(put)
      })

      htable.close()
      conn.close()
    }

  }

  /**
    * creates a put using the given key as the rowkey, and the map as the columns.  We assume
    * the CF is "d", and the map has a "key" value to use as the rowkey
    *
    * @param record
    * @param key
    * @return
    */
  def makePut(record: mutable.Map[String, String], cf: Array[Byte]): Put = {
    val put = new Put(record("key").getBytes)
    record.seq.filter(_._1 != "key").foreach(t => {
      put.addColumn(cf, t._1.getBytes(), t._2.getBytes())
    })
    put
  }

}