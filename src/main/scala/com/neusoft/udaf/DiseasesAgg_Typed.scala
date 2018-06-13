package com.neusoft.udaf

import com.neusoft.extension.{AggCustomer, Customer, Diagnose}
import com.neusoft.implicits._
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.{Aggregator}

import scala.collection.mutable.ArrayBuffer

object DiseasesAgg_Typed extends Aggregator[Customer, AggCustomer, AggCustomer] {

  override def zero: AggCustomer = AggCustomer(null, null, null, null, null, null, ArrayBuffer())

  override def reduce(b: AggCustomer, a: Customer): AggCustomer = {
    b.birthday = a.birthday
    b.card_no = a.card_no
    b.home_tel = a.home_tel
    b.name = a.name
    b.register_date = a.register_date
    b.sex = a.sex
    b.diseases ++= a.diagnose.extractDiagnose.map(diagnose => Diagnose(diagnose, a.oper_date))
    b
  }

  override def merge(b1: AggCustomer, b2: AggCustomer): AggCustomer = {
    b1.diseases ++= b2.diseases
    b1
  }

  override def finish(reduction: AggCustomer): AggCustomer = reduction

  override def bufferEncoder: Encoder[AggCustomer] = Encoders.product

  override def outputEncoder: Encoder[AggCustomer] = Encoders.product

}
