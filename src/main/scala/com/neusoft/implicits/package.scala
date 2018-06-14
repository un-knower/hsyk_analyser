package com.neusoft

import com.neusoft.extension.{DiagnoseStr, JsonStr}

/**
  * Created by ZhangQiang 2018/5/8 15:29
  */
package object implicits {

  implicit def string2DiagnoseStr(str: String): DiagnoseStr = new DiagnoseStr(str)

  implicit def string2JsonStr(str: String): JsonStr = new JsonStr(str)

}
