package com.neusoft

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.NShort.NShortSegment
import com.hankcs.hanlp.tokenizer.{NLPTokenizer, SpeedTokenizer}

object HanlpTest {
  def main(args: Array[String]): Unit = {
    // val text = "吉林省白城市洮南市团结办事处十一委八十三组"
    val text = "壮族自治区"
    // N-最短路径分词
    val nShortSegment = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true)
    println(s"N-最短路径分词 ${nShortSegment.seg(text)}")
    // NLP分词
    println(s"NLP分词 ${NLPTokenizer.ANALYZER.segment(text)}")
    // 底层实际调用 NLPTokenizer.ANALYZER.seg(text)
    println(s"NLP分词 ${NLPTokenizer.segment(text)}")
    // 极速词典分词
    println(s"极速词典分词 ${SpeedTokenizer.segment(text)}")
    // 标准分词
    println(s"标准分词-enablePlaceRecognize ${HanLP.segment(text)}")
    // 标准分词-enablePlaceRecognize
    println(s"标准分词-enablePlaceRecognize ${HanLP.newSegment().enablePlaceRecognize(true).seg(text)}")

//    println(AddressProcessor.removeDuplicate("沈阳皇姑长江北街3-3号351"))


    import com.hankcs.hanlp.HanLP
    // 打开HanLP调试模式查看DEBUG信息
    HanLP.Config.enableDebug()
    //    println(searchMap("大连市").parentNode.addressName)
    //    searchMap("大连市").childNode.foreach(node => print(node.addressName + " "))
    //    println(splitAddress("北京丰台"))
    //    println(splitAddress("重庆渝北"))
    //    println(splitAddress("沈阳市法库县双台子乡石头房村"))
    //    println(splitAddress("通辽"))
    //    // TODO 西藏
    //    println(splitAddress("壮族自治区"))
    //    println(splitAddress("阜新市彰武县二道河子乡腰窝堡村东宋家屯"))
    //    println(splitAddress(removeDuplicate("沈阳市大东区沈阳市大东区富强社区新民小区131栋10号1")))
    //    println(splitAddress(removeDuplicate("内蒙古内蒙古通辽市科尔泌乡不详")))
    //    println(splitAddress("吉林省四平市二道河子乡腰窝堡村东宋家屯"))
    //    println(splitAddress("白城洮南团结办事处十一委八十三组"))
    //    println(splitAddress("沈阳市新民市东蛇山子乡"))
    //    println(splitAddress("沈阳市东陵区"))
    //    println(splitAddress("沈阳市新民市三道岗"))
    //    println(splitAddress("沈阳市和平区澳门"))
    //    println(NLPTokenizer.ANALYZER.segment("沈阳市沈北新区湾社区"))

    import com.hankcs.hanlp.model.perceptron.PerceptronLexicalAnalyzer
    val analyzer = new PerceptronLexicalAnalyzer
    //    analyzer.learn("沈阳市/ns 和平区/ns 澳门街/ns")
    import com.hankcs.hanlp.HanLP
    //    analyzer.getPerceptronSegmenter.getModel.save(HanLP.Config.PerceptronCWSModelPath)
    analyzer.learn("吉林省/ns 四平市/ns 二道河子乡/ns 腰窝堡村/ns 东宋家屯/ns")
    analyzer.getPerceptronSegmenter.getModel.save(HanLP.Config.PerceptronCWSModelPath)
    println(NLPTokenizer.ANALYZER.segment("吉林省四平市二道河子乡腰窝堡村东宋家屯"))
    System.out.println(analyzer.analyze("沈阳市和平区澳门街"))
    System.out.println(analyzer.analyze("吉林省四平市二道河子乡腰窝堡村东宋家屯"))

  }
}
