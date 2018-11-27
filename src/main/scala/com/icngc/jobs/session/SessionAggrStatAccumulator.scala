package com.icngc.jobs.session

import com.icngc.constant.Constants
import com.icngc.utils.StringUtils
import org.apache.spark.util.AccumulatorV2

/**
  * @author: Mr.Wang
  * @create: 2018-11-27 21:42
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String,String]{
  // result: 记录的是当前累加器的最终结果
  var result = Constants.AGGR_RESULT.toString // 初始值
  override def isZero: Boolean = true

  override def copy(): AccumulatorV2[String, String] = {
    val myAccumulator = new SessionAggrStatAccumulator()
    myAccumulator.result = this.result
    myAccumulator
  }

  override def reset(): Unit = {
    result = Constants.AGGR_RESULT.toString
  }

  override def add(v: String): Unit = {
    val v1 = result
    val k = v

    if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(k)){
      var newResult = ""
      // 从v1中提取k对应的值，并累加
      val oldValue = StringUtils.getFieldFromConcatString(v1,"\\|",k)

      if (oldValue != null){
        val newValue = oldValue.toInt +1
        newResult = StringUtils.setFieldInConcatString(v1,"\\|",k,String.valueOf(newValue))
      }
      newResult
    }
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case map: SessionAggrStatAccumulator => result = other.value
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: String = result
}
