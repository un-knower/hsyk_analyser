package com.neusoft.nlp

import java.io.{BufferedReader, InputStreamReader}

import com.google.gson.JsonParser
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.neusoft.nlp.AddressSeparator.TreeNode
import TextProcessor

import scala.collection.{Traversable, mutable}

/**
  * Create by ZhangQiang 2018/4/25 11:28
  */


/**
  * 将连续的地址字符串拆分成(省、市、县、社区)
  *
  * @param similarity_threshold 相似度阈值，用于匹配城市。默认 0.5
  */
class AddressSeparator(val similarity_threshold: Double = 0.5) extends Serializable {


  /**
    * 去掉重复的地名
    *
    * @param address 沈阳市铁西区沈阳市铁西区沈阳市铁西区滑翔小区
    * @return 沈阳市铁西区滑翔小区
    */
  private def removeDuplicate(address: String): String = {
    if (address == null || address == "") ""
    else {
      NLPTokenizer.ANALYZER.segment(address).toArray(Array[String]()).distinct.reduce(_ + _)
    }
  }

  /**
    * 根据候选城市/区地址节点列表，分析地址字符串所在的城市/区
    *
    * @param address              待分析的地址字符串：沈阳市法库县双台子乡石头房村
    * @param candidateAddressList 候选地址节点集合：城市节点列表/区县节点列表
    * @return
    */
  private def findSimilarityAddress(address: String, candidateAddressList: Traversable[TreeNode]): Option[TreeNode] = {
    // 无地址信息
    if (address == null || address == "") None
    // 由于地区数据缺失，没有候选节点信息
    else if (candidateAddressList.isEmpty) None
    else {


      /**
        * 首先将待分析的地址字符串进行分词，分词方式的选择直接影响算法的准确率。例如：
        * `NLPTokenizer.segment(text)`对于`吉林省白城市洮南市团结办事处十一委八十三组`分词效果极差。
        * 因为它实际调用的是`NLPTokenizer.ANALYZER.seg(text)`方法
        */
      val words = NLPTokenizer.ANALYZER.segment(address).toArray(Array[String]())
      val (destinationNode, similarity) =
        candidateAddressList
          .map {
            candidateAddressNode =>
              //              try {
              words
                // 计算当前候选地址和别名与各个分词的相似度
                .map { word =>

                /**
                  * 将待匹配地址去掉`市`可以减少误判率，例如：
                  * 辽宁省-沈阳市-新民市，江苏省-新余市，新民市和新余市的levinshtein相似度是66.7%，会出现误判。
                  * 但是去掉`市`之后，新民和新余市的levinshtein相似度是33.3%
                  */
                val processedWord = word.replaceAll("市", "")
                // 别名相似度
                var maxAliasSimilarity: Double = 0
                val addressAlias = candidateAddressNode.addressAlias
                if (addressAlias.nonEmpty) maxAliasSimilarity = addressAlias.map(TextProcessor.getLevenshteinSimilarity(processedWord, _)).max
                val addressNameSimilarity: Double = TextProcessor.getLevenshteinSimilarity(processedWord, candidateAddressNode.addressName)
                (candidateAddressNode, Math.max(maxAliasSimilarity, addressNameSimilarity))
              }
                // 取最高相似度，作为该候选节点的权重
                .maxBy(_._2)
          }
          // 取权重最高的候选节点为目标节点
          .maxBy(_._2)
      // 如果目标节点的权重没有超过阈值，说明候选集中没有符合的地址
      if (similarity > similarity_threshold) Some(destinationNode) else None
    }
  }

  /**
    * 将地址按省/市/区/街道拆分
    *
    * @param address
    * @return (省,市,区,街道)
    */
  def splitAddress(address: String): (String, String, String, String) = {

    val addr = removeDuplicate(address)

    // 所属省份
    var belongsProvinceName = ""
    // 所属省份别名
    var belongsProvinceAlias = ""
    // 所属城市
    var belongsCityName = ""
    // 所属区
    var belongsDistrictName = ""
    // 所属区别名
    var belongsDistrictAlias = ""

    // 查找所属城市
    val belongsCityInfo: Option[TreeNode] = findSimilarityAddress(addr, AddressSeparator.searchMap.values)

    if (belongsCityInfo.isDefined) {

      val belongsCityNode = belongsCityInfo.get
      // 所属区信息
      var belongsDistrictInfo: Option[TreeNode] = None

      // 判断是否是直辖市
      if (AddressSeparator.MUNICIPALITY.contains(belongsCityNode.addressName)) {
        // 直辖市即省份
        belongsProvinceName = belongsCityNode.addressName
        // 省份别名
        belongsProvinceAlias = belongsCityNode.addressAlias.mkString("")
        // 直辖市城市为空
        belongsCityName = ""

        belongsDistrictInfo = findSimilarityAddress(addr, belongsCityNode.childNode.head.childNode)
      }
      else {
        // 所属省份
        belongsProvinceName = belongsCityNode.parentNode.addressName
        // 省份别名
        belongsProvinceAlias = belongsCityNode.addressAlias.mkString("")
        // 所属城市
        belongsCityName = belongsCityNode.addressName

        belongsDistrictInfo = findSimilarityAddress(addr, belongsCityNode.childNode)
      }
      if (belongsDistrictInfo.isDefined) {
        // 所属区节点
        val belongsDistrictNode = belongsDistrictInfo.get

        belongsDistrictAlias = belongsDistrictNode.addressAlias.mkString("")
        // 所属区
        belongsDistrictName = belongsDistrictNode.addressName
      }
    }
    // 若无所属城市信息，查找省节点信息
    else {
      val belongsProvinceInfo = findSimilarityAddress(addr, AddressSeparator.tree.childNode)

      if (belongsProvinceInfo.isDefined) {
        val belongsProvinceNode = belongsProvinceInfo.get

        belongsProvinceName = belongsProvinceNode.addressName
        // 省份别名
        belongsProvinceAlias = belongsProvinceNode.addressAlias.mkString("")
      }

    }
    // 截取街道信息
    val belongsStreetName = addr.diff(addr.intersect(s"$belongsProvinceName$belongsProvinceAlias$belongsCityName$belongsDistrictName$belongsDistrictAlias"))

    (belongsProvinceName, belongsCityName, belongsDistrictName, belongsStreetName)
  }
}

object AddressSeparator {

  /**
    * 树节点
    *
    * @param addressName 节点名称
    */
  case class TreeNode(addressName: String) {

    var parentNode: TreeNode = _
    val childNode: mutable.MutableList[TreeNode] = mutable.MutableList()
    // 别名
    val addressAlias: mutable.MutableList[String] = mutable.MutableList()

    /**
      * 树结构展示
      *
      * @param index
      */
    def display(index: Int = 1): Unit = {
      println(s""" ${"---" * index}| $addressName""")
      childNode.foreach { node => node.display(index + 1) }
    }
  }

  // 直辖市
  private val MUNICIPALITY = Set("北京市", "天津市", "上海市", "重庆市", "香港特别行政区", "澳门特别行政区", "台湾省")

  /**
    * tree: TreeNode => 所有省市区节点构造的四层树
    * searchMap: Map[String, TreeNode] => 城市节点(包含直辖市和特别行政区)指针: (城市名称, 城市节点)
    */
  private val (tree, searchMap) = candidateAddressTreeInit("area.json")

  def apply(similarity_threshold: Double = 0.5): AddressSeparator = new AddressSeparator(similarity_threshold)

  /**
    * 候选地址节点树初始化
    *
    * @param path 候选地址 json 文件存放路径
    * @return
    */
  private def candidateAddressTreeInit(path: String): (TreeNode, Map[String, TreeNode]) = {

    // 城市节点指针，供搜索使用
    val citySearchMap = mutable.HashMap[String, TreeNode]()

    // 读取地域文件
    val bufferReader = new BufferedReader(new InputStreamReader(getClass.getClassLoader.getResourceAsStream(path)))
    var line: String = bufferReader.readLine()
    val buffer = new StringBuffer()
    while (line != null) {
      buffer.append(s"$line\n")
      line = bufferReader.readLine()
    }

    // 根节点
    val root: TreeNode = TreeNode("中国")

    // 根据json构造四层树
    val jsonArray = new JsonParser().parse(buffer.toString).getAsJsonArray
    import scala.collection.JavaConversions
    // 省级节点
    JavaConversions.asScalaIterator(jsonArray.iterator).foreach {
      provinceElem =>
        val jsonObj = provinceElem.getAsJsonObject
        // 省份名称(直辖市属于省份范围)
        val province = jsonObj.get("province").getAsString
        val provinceNode = TreeNode(province)
        // 省份别名
        val provinceAliasInfo = jsonObj.get("alias")
        if (provinceAliasInfo != null) {
          val provinceAliasArr = provinceAliasInfo.getAsJsonArray
          provinceNode.addressAlias ++= JavaConversions.asScalaIterator(provinceAliasArr.iterator).map(_.getAsString)
        }
        root.childNode += provinceNode
        provinceNode.parentNode = root

        // 省份下的所有城市(直辖市下没有城市级别的节点，用assistant代替)
        val cities = jsonObj.get("city_list")
        // 非直辖市省份
        if (cities != null) {
          // 市级节点
          JavaConversions.asScalaIterator(cities.getAsJsonArray.iterator)
            .foreach {
              cityElem =>
                val cityJsonObj = cityElem.getAsJsonObject
                val cityNode: TreeNode = TreeNode(cityJsonObj.get("city").getAsString)
                provinceNode.childNode += cityNode
                cityNode.parentNode = provinceNode
                // 供之后的市级搜索
                citySearchMap.put(cityNode.addressName, cityNode)
                // 区县级节点
                val districtJsonArr = cityJsonObj.get("country_list").getAsJsonArray
                JavaConversions.asScalaIterator(districtJsonArr.iterator)
                  .foreach {
                    districtElem =>
                      // 区县主名称
                      val district = districtElem.getAsJsonObject
                      val districtNode = TreeNode(district.get("name").getAsString)
                      /**
                        * 区县别名信息 ：
                        * 因为有的区县现在已经合并了，但是历史数据中的区县名称不相同
                        * 例如：现在辽宁省沈阳市的`浑南新区`和`东陵区`合并成了`浑南区`
                        */
                      val addressAliasElem = district.get("alias")
                      if (addressAliasElem != null) {
                        val addressAliasArr = addressAliasElem.getAsJsonArray
                        districtNode.addressAlias ++= JavaConversions.asScalaIterator(addressAliasArr.iterator).map(_.getAsString)
                      }
                      cityNode.childNode += districtNode
                      districtNode.parentNode = cityNode
                  }
            }
        }
        // 直辖市
        else {
          // 将直辖市添加市级搜索，供之后的市级搜索
          citySearchMap.put(provinceNode.addressName, provinceNode)
          // 市级节点
          val assistant: TreeNode = TreeNode("assistant")
          assistant.parentNode = provinceNode
          provinceNode.childNode += assistant
          // 区县级节点
          val districtJsonArr = jsonObj.get("country_list").getAsJsonArray
          JavaConversions.asScalaIterator(districtJsonArr.iterator)
            .foreach {
              districtElem =>
                // 区县主名称
                val district = districtElem.getAsJsonObject
                val districtNode = TreeNode(district.get("name").getAsString)
                /**
                  * 区县别名信息 ：
                  * 因为有的区县现在已经合并了，但是历史数据中的区县名称不相同
                  * 例如：现在辽宁省沈阳市的`浑南新区`和`东陵区`合并成了`浑南区`
                  */
                val addressAliasElem = district.get("alias")
                if (addressAliasElem != null) {
                  val addressAliasArr = addressAliasElem.getAsJsonArray
                  districtNode.addressAlias ++= JavaConversions.asScalaIterator(addressAliasArr.iterator).map(_.getAsString)
                }
                assistant.childNode += districtNode
                districtNode.parentNode = assistant
            }
        }
    }
    (root, citySearchMap.toMap)
  }
}