package conf

import java.util.Properties

object ConfigurationManager {
  var prop: Properties = null

  def initProperties(): Unit = {
    prop = new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream("my.properties")
    prop.load(in)
  }

  def getProperty(str: String): String = {
    if (null != prop) {
      prop.getProperty(str)
    }
    ""
  }

  def getInteger(str: String): Integer = {
    val value = getProperty(str)
    Integer.valueOf(value)
  }
}
