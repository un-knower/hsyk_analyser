//package com.neusoft.apps
//
//
//import java.util.Properties
//
//import com.neusoft.util.HBaseUtil
//import org.apache.hadoop.hbase.mapred.TableOutputFormat
//import org.apache.hadoop.mapred.JobConf
//import org.apache.spark.rdd.PairRDDFunctions
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.mutable
//
///**
//  * Created by ZhangQiang 2018/4/25 10:28
//  */
//object AddressSeparator {
//
//
//  def main(args: Array[String]): Unit = {
//
//    val sparkSession = SparkSession.builder.appName("Split address field").getOrCreate()
//
//    val jdbcProps = new Properties()
//    jdbcProps.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))
//
//    val jdbcURL = jdbcProps.getProperty("url")
//
//    val registerTable = "FIN_OPR_REGISTER"
//    // valid_flag 是数据校验字段：0退费,1有效,2作废
//    val registerNumSQL = s"(SELECT count(1) AS total FROM $registerTable t WHERE t.valid_flag = '1')"
//    val df = sparkSession.read.jdbc(jdbcURL, registerNumSQL, jdbcProps)
//    df.printSchema()
//    val registerNum = sparkSession.read.jdbc(jdbcURL, registerNumSQL, jdbcProps).head.getAs[java.math.BigDecimal]("TOTAL").longValue.toString
//
//    jdbcProps.put("numPartitions", "32")
//    jdbcProps.put("partitionColumn", "rn")
//    jdbcProps.put("lowerBound", "1")
//    jdbcProps.put("upperBound", registerNum)
//    jdbcProps.put("fetchsize", "500")
//
//    val registerSQL = s"(SELECT t.*, rownum AS rn FROM $registerTable t WHERE t.valid_flag = '1')"
//    // 门诊挂号表
//    val registerDF = sparkSession.read.jdbc(jdbcURL, registerSQL, jdbcProps)
//
//    val highValueInfoDF = registerDF.select(
//      "clinic_code", // 门诊挂号流水号
//      "card_no", // 客户卡号
//      "reg_date", // 挂号日期
//      "name", // 客户姓名
//      "birthday", // 客户生日
//      "sex_code", // 客户性别
//      "rela_phone", // 客户电话号
//      "address", // 客户住址
//      "pact_code", // 缴费类型号
//      "pact_name", // 缴费类型
//      "reglevl_code", // 挂号类型号
//      "reglevl_name" // 挂号类型
//    )
//
//    highValueInfoDF.printSchema()
//
//    val hadoopRDD = highValueInfoDF.rdd
//      .map { row =>
//        val dataList = new mutable.MutableList[(String, String, String)]()
//        val map = row.getValuesMap[Any](row.schema.fieldNames)
//        val rowkey = s"${map("card_no")}-${map("clinic_code")}"
//        val newMap = map - "card_no" - "clinic_code"
//        newMap.foreach {
//          case (key, value) =>
//            key match {
//              case "address" =>
//                if (value != null) {
//                  val address = AddressProcessor.removeDuplicate(value.toString)
//                  val (province, city, district, street) = AddressProcessor.splitAddress(address)
//                  dataList += (("info", "province", province))
//                  dataList += (("info", "city", city))
//                  dataList += (("info", "district", district))
//                  dataList += (("info", "street", street))
//                }
//              case _ =>
//                if (value != null) {
//                  dataList += (("info", key, value.toString))
//                }
//            }
//        }
//        HBaseUtil.convert2HBaseWritable(rowkey, dataList.toList)
//      }
//
//    val hbaseProps = new Properties()
//    hbaseProps.load(this.getClass.getClassLoader.getResourceAsStream("hbase.properties"))
//
//    val jobConf = new JobConf(this.getClass)
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    jobConf.set("hbase.mapred.outputtable", "hsyk_his_custom_info_test")
//    jobConf.set("hbase.zookeeper.quorum", hbaseProps.getProperty("hbase.zookeeper.quorum"))
//    new PairRDDFunctions(hadoopRDD).saveAsHadoopDataset(jobConf)
//
//  }
//
//}
