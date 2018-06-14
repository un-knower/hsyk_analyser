package com.neusoft.extension

class JsonStr(str: String) {
  def jsonFormat: String = {
    str.replaceAll("""\\""","""""").replace(s""""[""", """[""").replace(s"""]"""", """]""")
  }

  def strFormat: String = {
    str.replaceAll("""]""","""""").replaceAll("""[""","""""")
  }
}
