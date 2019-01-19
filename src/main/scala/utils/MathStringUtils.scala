package utils

object MathStringUtils {
  /**
    * 补全参数
    *
    * @param str
    * @return
    */
  def fulFullString(str: String): String = {
    if ("".equals(str) || null == str) {
      str
    } else {
      str.length match {
        case 2 => str
        case _ => "0" + str
      }
    }
  }

  /**
    * 截断字符串两侧的逗号
    *
    * @param str 字符串
    * @return 字符串
    */
  def trimComma(str: String): String = {
    var tmpStr: String = null
    if (str.startsWith(",")) tmpStr = str.substring(1)
    if (str.endsWith(",")) tmpStr = str.substring(0, str.length - 1)
    tmpStr
  }

  /**
    * 从拼接的字符串中提取字段
    *
    * @param str       字符串
    * @param delimiter 分隔符
    * @param field     字段
    * @return 字段值
    */
  def getFieldFromConcatString(str: String, delimiter: String, field: String): String = {
    try {
      val fields = str.split(delimiter)
      for (concatField <- fields) {
        val splitIterators = concatField.split("=")
        if (splitIterators.length == 2) {
          val fieldName = splitIterators(0)
          val fieldValue = splitIterators(1)
          if (fieldName == field)
            fieldValue
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }

  /**
    * 从拼接的字符串中给字段设置值
    *
    * @param str           字符串
    * @param delimiter     分隔符
    * @param field         字段名
    * @param newFieldValue 新的field值
    * @return 字段值
    */
  def setFieldInConcatString(str: String, delimiter: String, field: String, newFieldValue: String): String = {
    val fieldList = str.split(delimiter)
    for (f <- fieldList) {
      val valueList = f.split("=")
      if (field.equals(valueList(0))) {
        valueList(1) = newFieldValue
      }
    }
    str
  }
}
