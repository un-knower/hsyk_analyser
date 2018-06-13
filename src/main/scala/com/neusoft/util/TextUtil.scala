/*
 * Create by ZhangQiang 2018/4/25
 */
package com.neusoft.util

/**
  * Create by ZhangQiang 2018/4/25 13:08
  */
object TextUtil {
  /**
    * 获取两个字符串的利文斯顿距离
    *
    * @param str1
    * @param str2
    * @return
    */
  def getLevenshteinSimilarity(str1: String, str2: String): Double = {
    val n = str1.length
    val m = str2.length
    val matrix: Array[Array[Int]] = Array.ofDim(n + 1, m + 1)
    for (i <- 0 to n) {
      for (j <- 0 to m) {
        if (i == 0) matrix(i)(j) = j
        else if (j == 0) matrix(i)(j) = i
        else {
          var f: Int = 0
          val char_i = str1.charAt(i - 1)
          val char_j = str2.charAt(j - 1)
          if (char_i != char_j) f = 1
          matrix(i)(j) = Math.min(matrix(i - 1)(j - 1) + f, Math.min(matrix(i - 1)(j) + 1, matrix(i)(j - 1) + 1))
        }
      }
    }
    1 - matrix(n)(m).asInstanceOf[Double] / Math.max(n, m)
  }
}
