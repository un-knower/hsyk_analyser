package com.neusoft.es

import java.util.Properties

import com.google.gson.Gson
import com.neusoft.extension.Customer
import com.neusoft.util.{OracleTables, OracleUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object LoadCustomerData2ES {

  def main(args: Array[String]): Unit = {

    /* ================= 加载 ElasticSearch 配置 ================= */
    val esProps = new Properties()
    esProps.load(this.getClass.getClassLoader.getResourceAsStream("elasticsearch.properties"))

    val conf = new SparkConf()
    conf.set("es.index.auto.create", esProps.getProperty("es.index.auto.create"))
    conf.set("es.nodes", esProps.getProperty("es.nodes"))
    conf.set("es.port", esProps.getProperty("es.port"))

    val sparkSession = SparkSession.builder()
      .appName("何氏眼科二期 ：Load Basic Customer Data from Oracle to ElasticSearch")
      .config(conf)
      .master("local[16]")
      .getOrCreate()

    /* ================= 加载 JDBC 配置 ================= */
    val jdbcProps = new Properties()
    jdbcProps.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))

    /* ================= 读取客户基本信息信息表 ================= */

    val jdbcProps_basic = jdbcProps.clone().asInstanceOf[Properties]

    val basicNum = OracleUtil.getTableSize(sparkSession)(OracleTables.BASIC_T, jdbcProps)

    jdbcProps_basic.put("numPartitions", "32")
    jdbcProps_basic.put("partitionColumn", "rn")
    jdbcProps_basic.put("lowerBound", "1")
    jdbcProps_basic.put("upperBound", basicNum)
    jdbcProps_basic.put("fetchsize", "500")


    val basicFields = Set("CARD_NO", "NAME", "BIRTHDAY", "HOME", "HOME_TEL", "OPER_DATE")
    val basicDF = OracleUtil.getTableData(sparkSession)(OracleTables.BASIC_T, jdbcProps_basic, basicFields).withColumnRenamed("OPER_DATE", "REGISTER_DATE").drop("RN")

    import org.elasticsearch.spark._

    basicDF.rdd
      .map { row => row.getValuesMap(row.schema.fieldNames) }
      .saveToEs("hsyk_basic_info/docs")


  }
}
