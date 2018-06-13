package com.neusoft.udaf

import java.sql.Timestamp

import com.neusoft.extension.Diagnose
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import com.neusoft.implicits._

object DiseasesAgg extends UserDefinedAggregateFunction {

  // 指定具体输入列的数据类型
  override def inputSchema: StructType = StructType(
    //    StructField("CARD_NO", StringType) ::
    //      StructField("NAME", StringType) ::
    //      StructField("BIRTHDAY", StringType) ::
    //      StructField("HOME", StringType) ::
    //      StructField("HOME_TEL", StringType) ::
    //      StructField("REGISTER_DATE", StringType) ::
    //      StructField("SEX", StringType) ::
    StructField("DIAGNOSE", StringType) ::
      StructField("OPER_DATE", TimestampType) :: Nil)

  // 在进行聚合操作的时候buffer的数据类型
  override def bufferSchema: StructType = StructType(StructField("DISEASES", ArrayType(StructType(StructField("DIAGNOSE", StringType) :: StructField("OPER_DATE", DateType) :: Nil))) :: Nil)

  // UDAF函数计算的结果类型
  override def dataType: DataType = ArrayType(StructType(StructField("DIAGNOSE", StringType) :: StructField("OPER_DATE", DateType) :: Nil))

  // 是否确保一致性，即在相同输入的情况下有相同的输出
  override def deterministic: Boolean = true

  // 在Aggregate之前每组数据的初始化结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = Seq()

  // 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getSeq(0) ++: input.getString(0).extractDiagnose().map(Diagnose(_, input.getAs[Timestamp](1)))
  }

  // 最后在分布式节点进行Local Reduce完成后需要进行全局级别的Merge操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = buffer1.getSeq(0) ++: buffer2.getSeq(0)

  // 返回UDAF最后的计算结果
  override def evaluate(buffer: Row): Seq[Diagnose] = buffer.getSeq(0)
}
