package com.neusoft.extension

import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer

case class AggCustomer(var card_no: String, var name: String, var birthday: Timestamp, var home_tel: String, var register_date: Timestamp, var sex: String, var diseases: ArrayBuffer[Diagnose])

