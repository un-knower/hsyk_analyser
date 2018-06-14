package com.neusoft.es

import java.util.Properties

import com.google.gson.{Gson, JsonParser}
import com.neusoft.udaf.DiseasesAgg
import com.neusoft.util.{OracleTables, OracleUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object CustomerDiseaseDistributionData2ES {

  def main(args: Array[String]): Unit = {

    /* ================= 加载 ElasticSearch 配置 ================= */
    val esProps = new Properties()
    esProps.load(this.getClass.getClassLoader.getResourceAsStream("elasticsearch.properties"))

    val conf = new SparkConf()
    conf.set("es.index.auto.create", esProps.getProperty("es.index.auto.create"))
    conf.set("es.nodes", esProps.getProperty("es.nodes"))
    conf.set("es.port", esProps.getProperty("es.port"))

    val sparkSession = SparkSession.builder()
      .appName("何氏眼科二期 ：Load Disease Data from Oracle to ElasticSearch")
      .config(conf)
      .master("local[16]")
      .getOrCreate()

    /* ================= 加载 JDBC 配置 ================= */
    val jdbcProps = new Properties()
    jdbcProps.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))

    /* ================= 读取疾病信息表 ================= */
    val diseaseNum = OracleUtil.getTableSize(sparkSession)(OracleTables.DISEASE_T, jdbcProps)

    val jdbcProps_diseases = jdbcProps.clone().asInstanceOf[Properties]

    jdbcProps_diseases.put("numPartitions", "32")
    jdbcProps_diseases.put("partitionColumn", "rn")
    jdbcProps_diseases.put("lowerBound", "1")
    jdbcProps_diseases.put("upperBound", diseaseNum)
    jdbcProps_diseases.put("fetchsize", "500")

    val diseaseFields = Array("CARD_NO", "SEX", "DIAGNOSE", "OPER_DATE")
    val diseaseDF = OracleUtil.getTableData(sparkSession)(OracleTables.DISEASE_T, jdbcProps_diseases, diseaseFields).withColumnRenamed("CARD_NO", "CARD_NO_D").drop("RN")

    println(s"Disease Table Size = ${diseaseDF.count()}")

    // val deseases = diseaseDF.filter(diseaseDF("IS_VALID") === "1")

    /* ================= 读取客户基本信息信息表 ================= */
    val basicNum = OracleUtil.getTableSize(sparkSession)(OracleTables.BASIC_T, jdbcProps)

    val jdbcProps_basic = jdbcProps_diseases.clone().asInstanceOf[Properties]
    jdbcProps_basic.put("upperBound", basicNum)

    val basicFields = Array("CARD_NO", "NAME", "BIRTHDAY", "HOME", "HOME_TEL", "OPER_DATE")
    val basicDF = OracleUtil.getTableData(sparkSession)(OracleTables.BASIC_T, jdbcProps_diseases, basicFields).withColumnRenamed("OPER_DATE", "REGISTER_DATE").drop("RN")

    println(s"Cutomer Basic Table Size = ${basicDF.count()}")

    /* ================= 表连接 ================= */
    val customerDistribution = basicDF.join(diseaseDF, basicDF("CARD_NO") === diseaseDF("CARD_NO_D")).drop("CARD_NO_D")

    println(s"customerDistribution Table Size = ${customerDistribution.count()}")

    sparkSession.udf.register("diseasesAgg", DiseasesAgg)
    customerDistribution.createOrReplaceTempView("customer_distribution")

    import org.elasticsearch.spark._

    val jsonDF = sparkSession.sql(s"select diseasesAgg(CARD_NO, NAME, BIRTHDAY, HOME, HOME_TEL, REGISTER_DATE, SEX, DIAGNOSE, OPER_DATE) as JSON from customer_distribution group by CARD_NO")
    println(s"jsonDF Table Size = ${jsonDF.count()}")
    //    jsonDF.rdd.take(1).map(row => println(row.getAs[String]("JSON")))

    jsonDF.rdd.map(row => row.getAs[String]("JSON")).saveJsonToEs(IndexDict.DISEASE_DISTRIBUTION)

  }
}
