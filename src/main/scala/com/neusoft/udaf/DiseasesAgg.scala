package com.neusoft.udaf

import scala.collection.JavaConverters._
import com.google.gson.Gson
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import com.neusoft.implicits._
import java.util.{Map => JavaMap}

import com.neusoft.nlp.AddressSeparator

import scala.collection.mutable


object DiseasesAgg extends UserDefinedAggregateFunction {

  // 指定具体输入列的数据类型
  override def inputSchema: StructType = StructType(
    StructField("card_no", StringType) ::
      StructField("name", StringType) ::
      StructField("birthday", StringType) ::
      StructField("home", StringType) ::
      StructField("home_tel", StringType) ::
      StructField("register_date", StringType) ::
      StructField("sex", StringType) ::
      StructField("diagnose", StringType) ::
      StructField("oper_date", StringType) :: Nil)

  // 在进行聚合操作的时候buffer的数据类型
  override def bufferSchema: StructType = StructType(StructField("diseases", MapType(StringType, StringType)) :: Nil)

  // UDAF函数计算的结果类型
  override def dataType: DataType = StringType

  // 是否确保一致性，即在相同输入的情况下有相同的输出
  override def deterministic: Boolean = true

  // 在Aggregate之前每组数据的初始化结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, String]()
  }


  // 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val diagnose = input.getString(7)

    if (diagnose != null) {
      // 拆分疾病字段
      val diseases: Array[JavaMap[String, String]] = diagnose
        .extractDiagnose()
        .map { diagnose => Map("diagnose" -> diagnose, "oper_date" -> input.getString(8)).asJava }
      val gson = new Gson()
      val bufferMap = buffer.getMap[String, String](0)
      // 用于存放聚合后的数据
      val aggMap = new mutable.HashMap[String, String]()
      // 初始化
      if (bufferMap.get("diseases").isEmpty) {
        val addressSeparator = AddressSeparator()
        val address = Option(input.getString(3)).getOrElse("").strFormat
        val (province, city, district, street) = addressSeparator.splitAddress(address)
        aggMap.put("card_no", input.getString(0))
        aggMap.put("name", Option(input.getString(1)).getOrElse("").strFormat)
        aggMap.put("birthday", input.getString(2))
        aggMap.put("home", address)
        aggMap.put("province", province)
        aggMap.put("city", city)
        aggMap.put("district", district)
        aggMap.put("street", street)
        aggMap.put("home_tel", Option(input.getString(4)).getOrElse("").strFormat)
        aggMap.put("register_date", input.getString(5))
        aggMap.put("sex", input.getString(6))
        aggMap.put("diseases", gson.toJson(diseases))
      } else {
        aggMap ++= bufferMap
        val aggDiseases: Array[JavaMap[String, String]] = gson.fromJson(aggMap("diseases"), classOf[Array[JavaMap[String, String]]]) ++: diseases
        aggMap.put("diseases", gson.toJson(aggDiseases))
      }

      buffer(0) = aggMap.toMap
    }

  }

  // 最后在分布式节点进行Local Reduce完成后需要进行全局级别的Merge操作
  override def merge(buffer: MutableAggregationBuffer, merge: Row): Unit = {

    val resultMap = new mutable.HashMap[String, String]()
    val bufferMap = buffer.getMap[String, String](0)
    val mergeMap = merge.getMap[String, String](0)

    if (bufferMap.isEmpty) {
      resultMap ++= mergeMap
    } else {
      val gson = new Gson()
      val diseases = gson.fromJson(bufferMap("diseases"), classOf[Array[JavaMap[String, String]]]) ++:
        gson.fromJson(mergeMap("diseases"), classOf[Array[JavaMap[String, String]]])
      resultMap.put("diseases", gson.toJson(diseases))
    }

    buffer(0) = resultMap.toMap
  }

  // 返回UDAF最后的计算结果
  override def evaluate(buffer: Row): String = {
    new Gson().toJson(buffer.getJavaMap(0)).jsonFormat
  }
}
