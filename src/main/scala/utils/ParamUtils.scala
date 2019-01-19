package utils

import com.alibaba.fastjson.{JSONArray, JSONObject}

object ParamUtils {
  def getTaskIdFromArgs(args: Array[String]): Long = {
    if (!args.isEmpty) {
      args(0).toLong
    }
    0L
  }

  def getParams(jsonObject: JSONObject, field: String): String = {
    val jsonArray = jsonObject.getJSONArray(field)
    if (jsonArray != null
      && jsonArray.size > 0) {
      jsonArray.getString(0)
    }
    null
  }

}
