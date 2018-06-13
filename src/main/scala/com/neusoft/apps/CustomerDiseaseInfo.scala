package com.neusoft.apps

import java.sql.Timestamp
import java.util.Properties

import com.neusoft.util.HBaseUtil
import org.apache.spark.sql.SparkSession


/**
  * Create by ZhangQiang 2018/5/8 12:42
  */
object CustomerDiseaseInfo {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.appName("Customer disease information load").getOrCreate

    val diseaseT_Oracle = "met_cas_history"
    val diseaseT_HBase = "hsyk_his_custom_disease_test"

    val jdbcProps = new Properties()
    jdbcProps.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))
    val jdbcURL = jdbcProps.getProperty("url")

    val diseaseSQL = s"(SELECT t.card_no, t.diagnose, t.oper_date FROM $diseaseT_Oracle t)"
    val diseaseDF = sparkSession.read.jdbc(jdbcURL, diseaseSQL, jdbcProps)

    import com.neusoft.implicits._

    val hadoopRDD = diseaseDF.rdd.flatMap {
      row =>
        val cardNo = row.getAs[String]("CARD_NO")
        val operDate = row.getAs[Timestamp]("OPER_DATE").toString.split(" ")(0)
        val rawDiagnose = row.getAs[String]("DIAGNOSE")
        // 一条记录中包含多条疾病信息，拆分成多条记录，一个疾病一条
        val indexedDiagnoses = rawDiagnose.extractDiagnose()
        indexedDiagnoses.map {
          diagnose =>
            val rowkey = s"$operDate:$cardNo:${indexedDiagnoses.indexOf(diagnose)}"
            val dataList = List(
              ("INFO", "CARD_NO", cardNo),
              ("INFO", "OPER_DATE", operDate),
              ("INFO", "DIAGNOSE", diagnose)
            )
            HBaseUtil.convert2HBaseWritable(rowkey, dataList)
        }
    }

    HBaseUtil.save2HBase(diseaseT_HBase)(hadoopRDD)
  }

}
