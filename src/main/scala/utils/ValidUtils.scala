package utils

object ValidUtils {

  /**
    * 比较验证参数是否在同一个区间内
    *
    * @param str        用户信息拼接的参数
    * @param validKey   验证的参数的key
    * @param validParam 用于验证的拼接参数
    * @param judgeStart 用于验证的开始key
    * @param judgeEnd   用于验证的结束的key
    * @return 返回是否在一个区间内
    */
  def between(str: String, validKey: String, validParam: String, judgeStart: String, judgeEnd: String): Boolean = {
    val realValue: String = MathStringUtils.getFieldFromConcatString(str, "|", validKey)
    val realJudgeStart = MathStringUtils.getFieldFromConcatString(validParam, "|", judgeStart)
    val realJudgeEnd = MathStringUtils.getFieldFromConcatString(validParam, "|", judgeEnd)
    if (realJudgeStart.isEmpty || realJudgeEnd.isEmpty) {
      return true
    }
    val realJudgeStartInt = Integer.valueOf(realJudgeStart)
    val realJudgeEndInt = Integer.valueOf(realJudgeEnd)
    if (!realValue.isEmpty) {
      val realValueInt = Integer.valueOf(realValue)
      if (realValueInt >= realJudgeStartInt
        && realValueInt <= realJudgeEndInt) {
        return true
      }
    }
    false
  }

  /**
    * 比较验证参数是否在同一个区间内
    *
    * @param str        用户信息拼接的参数
    * @param validKey   验证的参数的key
    * @param validParam 用于验证的拼接参数
    * @param judgeKey   用于验证的开始key
    * @return 返回是否在一个区间内
    */
  def in(str: String, validKey: String, validParam: String, judgeKey: String): Boolean = {
    val validValue: String = MathStringUtils.getFieldFromConcatString(validParam, "|", judgeKey)
    if (validValue.isEmpty) {
      return true
    }
    val validValueList: Array[String] = validValue.split(",")
    val realValue = MathStringUtils.getFieldFromConcatString(str, "|", validKey)
    if (!realValue.isEmpty) {
      for (value <- validValueList) {
        if (realValue.equals(value)) {
          return true
        }
      }
    }
    false
  }
}
