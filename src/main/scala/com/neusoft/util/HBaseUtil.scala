package com.neusoft.util

import java.util.Properties

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

/**
  * Create by ZhangQiang 2018/4/25 14:01
  */
object HBaseUtil {

  /**
    * 将数据转换成可写入HBase的格式
    *
    * @param rowkey
    * @param columnValues
    * @return
    */
  def convert2HBaseWritable(rowkey: String, columnValues: List[(String, String, String)]): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(rowkey))
    columnValues.foreach { data =>
      val (family, qualifier, value) = data
      p.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))
    }
    (new ImmutableBytesWritable, p)
  }

  /**
    *
    * @param tableName
    * @param hadoopRDD
    */
  def save2HBase(tableName: String)(hadoopRDD: RDD[(ImmutableBytesWritable, Put)]) = {
    val hbaseProps = new Properties()
    hbaseProps.load(this.getClass.getClassLoader.getResourceAsStream("hbase.properties"))

    val jobConf = new JobConf(this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set("hbase.mapred.outputtable", tableName)
    jobConf.set("hbase.zookeeper.quorum", hbaseProps.getProperty("hbase.zookeeper.quorum"))
    new PairRDDFunctions(hadoopRDD).saveAsHadoopDataset(jobConf)
  }
}
