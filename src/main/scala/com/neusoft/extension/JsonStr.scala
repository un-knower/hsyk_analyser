package com.neusoft.extension

class JsonStr(str: String) {
  def jsonFormat: String = {
    str.replaceAll("""\\""","").replace(""""[""", "[").replace(s"""]"""", "]")
  }

  def strFormat: String = {
    str.replaceAll("""\]""","").replaceAll("""\[""","")
  }
}
