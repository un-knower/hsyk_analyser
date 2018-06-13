package com.neusoft.util

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object OracleUtil {

  def getTableSize(sparkSession: SparkSession)(tableName: String, jdbcProps: Properties): String = {
    val jdbcURL = jdbcProps.getProperty("url")
    val sql = s"""(SELECT count(1) as TOTAL FROM $tableName)"""
    sparkSession.read.jdbc(jdbcURL, sql, jdbcProps).head.getAs[java.math.BigDecimal]("TOTAL").longValue.toString
  }

  def getTableData(sparkSession: SparkSession)(tableName: String, jdbcProps: Properties, fields: Set[String] = Set()): DataFrame = {
    val jdbcURL = jdbcProps.getProperty("url")
    var fields_Str = "t.*"
    if (fields.nonEmpty) fields_Str = fields.map(field => s"t.$field").mkString(", ")
    val sql = s"""(SELECT $fields_Str, rownum as RN FROM $tableName t)"""
    println(sql)
    sparkSession.read.jdbc(jdbcURL, sql, jdbcProps)
  }
}
