package com.neusoft.extension

/**
  * Created by ZhangQiang 2018/5/8 15:28
  */
class DiagnoseStr(var str: String) {

  def extractDiagnose(): Array[String] = {
    this.standardizeRawDiagnose().toString.split("\\|").filterNot("" == _)
  }

  private def standardizeRawDiagnose(): DiagnoseStr = {
    this.standardizeBrackets().removeContentInBrackets().removePeriods()
  }

  private def standardizeBrackets(): DiagnoseStr = {
    this.str = str
      .replaceAll("（", "(")
      .replaceAll("）", ")")
      .replaceAll("""\[""", "(")
      .replaceAll("""\]""", ")")
      .replaceAll("【", "(")
      .replaceAll("】", ")")
    this
  }

  private def removePeriods(): DiagnoseStr = {
    this.str = str.replaceAll("""\.""", "")
    this
  }

  private def removeContentInBrackets(): DiagnoseStr = {
    this.str = str.replaceAll("""\(.*?\)""", "")
    this
  }

  override def toString: String = this.str
}