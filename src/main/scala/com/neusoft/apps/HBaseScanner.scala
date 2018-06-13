package com.neusoft.apps

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions

/**
  * Create by ZhangQiang 2018/4/25 14:28
  */
object HBaseScanner {

  @transient
  lazy val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    val sparkSession = SparkSession.builder().config(sparkConf).appName("HBase Scanner").getOrCreate
    val sparkContext = sparkSession.sparkContext
    val tableName = "hsyk_his_custom_info_test"
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.zookeeper.quorum", "server202,server203,server204")
    // Note: TableInputFormat 是 mapreduce 包下的
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)


    // 将整张表读入 Spark 并转换成 RDD
    val resultRDD: RDD[(ImmutableBytesWritable, Result)] = sparkContext.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    // 8 + 12 * 2 = 32
    resultRDD.repartition(32).foreach {
      case (_, result) =>
        val rowkey = Bytes.toString(result.getRow)
        val province = Bytes.toString(result.getValue("info".getBytes, "province".getBytes))
        val city = Bytes.toString(result.getValue("info".getBytes, "city".getBytes))
        val district = Bytes.toString(result.getValue("info".getBytes, "district".getBytes))
        val street = Bytes.toString(result.getValue("info".getBytes, "street".getBytes))
        logger.info(s"rowkey = $rowkey, province : $province, city : $city, district : $district, street : $street")

        val familyInfoMap = result.getFamilyMap("info".getBytes())
        JavaConversions.mapAsScalaMap(familyInfoMap).foreach {
          case (keyBytes, valueBytes) =>
            logger.info(s"${Bytes.toString(keyBytes)} : ${Bytes.toString(valueBytes)}")
        }
    }
  }

}
