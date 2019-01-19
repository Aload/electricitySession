package session

import common.Constants
import org.apache.spark.util.AccumulatorV2
import utils.MathStringUtils

/**
  * isZero: 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序。
  * copy: 拷贝一个新的AccumulatorV2
  * reset: 重置AccumulatorV2中的数据
  * add: 操作数据累加方法实现
  * merge: 合并数据
  * value: AccumulatorV2对外访问的数据结果
  */
class SessionAccumulator extends AccumulatorV2[String, String] {
  var result = Constants.SESSION_COUNT + "=0|" +
    Constants.TIME_PERIOD_1s_3s + "=0|" +
    Constants.TIME_PERIOD_4s_6s + "=0|" +
    Constants.TIME_PERIOD_7s_9s + "=0|" +
    Constants.TIME_PERIOD_10s_30s + "=0|" +
    Constants.TIME_PERIOD_30s_60s + "=0|" +
    Constants.TIME_PERIOD_1m_3m + "=0|" +
    Constants.TIME_PERIOD_3m_10m + "=0|" +
    Constants.TIME_PERIOD_10m_30m + "=0|" +
    Constants.TIME_PERIOD_30m + "=0|" +
    Constants.STEP_PERIOD_1_3 + "=0|" +
    Constants.STEP_PERIOD_4_6 + "=0|" +
    Constants.STEP_PERIOD_7_9 + "=0|" +
    Constants.STEP_PERIOD_10_30 + "=0|" +
    Constants.STEP_PERIOD_30_60 + "=0|" +
    Constants.STEP_PERIOD_60 + "=0"

  override def isZero: Boolean = {
    true
  }

  override def copy(): AccumulatorV2[String, String] = {
    val sessionAccumulator = new SessionAccumulator
    sessionAccumulator.result = this.result
    sessionAccumulator
  }

  override def reset(): Unit = {
    result = Constants.SESSION_COUNT + "=0|" +
      Constants.TIME_PERIOD_1s_3s + "=0|" +
      Constants.TIME_PERIOD_4s_6s + "=0|" +
      Constants.TIME_PERIOD_7s_9s + "=0|" +
      Constants.TIME_PERIOD_10s_30s + "=0|" +
      Constants.TIME_PERIOD_30s_60s + "=0|" +
      Constants.TIME_PERIOD_1m_3m + "=0|" +
      Constants.TIME_PERIOD_3m_10m + "=0|" +
      Constants.TIME_PERIOD_10m_30m + "=0|" +
      Constants.TIME_PERIOD_30m + "=0|" +
      Constants.STEP_PERIOD_1_3 + "=0|" +
      Constants.STEP_PERIOD_4_6 + "=0|" +
      Constants.STEP_PERIOD_7_9 + "=0|" +
      Constants.STEP_PERIOD_10_30 + "=0|" +
      Constants.STEP_PERIOD_30_60 + "=0|" +
      Constants.STEP_PERIOD_60 + "=0"
  }

  override def add(v: String): Unit = {
    if (!v.isEmpty) {
      val value: String = MathStringUtils.getFieldFromConcatString(result, "|", v)
      var valueInt = Integer.valueOf(value)
      valueInt = valueInt + 1
      result = MathStringUtils.setFieldInConcatString(result, "|", v, String.valueOf(valueInt))
    }
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {
    other match {
      case acc: SessionAccumulator =>
        result = other.value
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}"
    }
  }

  override def value: String = {
    result
  }
}
