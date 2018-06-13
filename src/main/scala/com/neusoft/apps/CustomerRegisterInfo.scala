package com.neusoft.apps

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Create by ZhangQiang 2018/5/8 13:04
  */
object CustomerRegisterInfo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.appName("Customer register information load").getOrCreate()

    val registerT_Oracle = "fin_opr_register"
    val registerT_HBase = "hsyk_his_custom_register_test"

    val jdbcProps = new Properties()
    jdbcProps.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))

    val jdbcURL = jdbcProps.getProperty("url")
  }
}
