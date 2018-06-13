package com.neusoft.apps

import java.util.Properties

import com.neusoft.nlp.AddressSeparator
import com.neusoft.util.HBaseUtil
import org.apache.spark.sql.SparkSession


/**
  * Create by ZhangQiang 2018/5/8 12:38
  */
object CustomerBasicInfo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.appName("Customer basic information load").getOrCreate()

    val basicT_Oracle = "fin_opr_register"
    val basicT_HBase = "hsyk_his_custom_info_test"

    val jdbcProps = new Properties()
    jdbcProps.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))

    val jdbcURL = jdbcProps.getProperty("url")

    // valid_flag 是数据校验字段：0退费,1有效,2作废
    val registerNumSQL = s"(SELECT count(1) AS total FROM $basicT_Oracle t WHERE t.valid_flag = '1')"
    val registerNum = sparkSession.read.jdbc(jdbcURL, registerNumSQL, jdbcProps).head.getAs[java.math.BigDecimal]("TOTAL").longValue.toString

    jdbcProps.put("numPartitions", "32")
    jdbcProps.put("partitionColumn", "rn")
    jdbcProps.put("lowerBound", "1")
    jdbcProps.put("upperBound", registerNum)
    jdbcProps.put("fetchsize", "500")

    val registerSQL =s"""(SELECT t.card_no, t.name, t.birthday, t.sex_code, t.address, t.rela_phone, rownum AS rn FROM $basicT_Oracle t WHERE t.valid_flag = '1')"""

    // 注：这样读出来的 schema 和 oracle 库中的字段相同，oracle 字段如果全是大写，那么这里全是大写
    // 使用 DataFrame.select("小写字段名")，那么新的 DataFrame.schema 就会变成小写。
    // DataFrame.select() 方法是大小写不敏感的
    val customerDF = sparkSession.read.jdbc(jdbcURL, registerSQL, jdbcProps)

    val addressSeparator = new AddressSeparator

    val hadoopRDD = customerDF.rdd
      .map { row =>
        // 大小写敏感
        val rowkey = row.getAs[String]("CARD_NO")

        val dataList = row.getValuesMap[Any](row.schema.fieldNames)
          .flatMap {
            case (key, value) =>
              key match {
                case "ADDRESS" =>
                  if (value != null) {
                    val (province, city, district, street) = addressSeparator.splitAddress(value.toString)
                    List(
                      // 注意：HBase 字段是大小写敏感的
                      ("INFO", "PROVINCE", province),
                      ("INFO", "CITY", city),
                      ("INFO", "DISTRICT", district),
                      ("INFO", "STREET", street)
                    )
                  } else {
                    List()
                  }
                case _ =>
                  if (value != null) List(("INFO", key, value.toString))
                  else List()
              }
          }
          .toList

        HBaseUtil.convert2HBaseWritable(rowkey, dataList)
      }

    HBaseUtil.save2HBase(basicT_HBase)(hadoopRDD)
  }
}
