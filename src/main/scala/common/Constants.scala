package common

object Constants {
  /**
    * 项目配置相关的常量
    */
  val JDBC_DRIVER = "jdbc.driver"
  val JDBC_DATASOURCE_SIZE = "jdbc.datasource.size"
  val JDBC_URL = "jdbc.url"
  val JDBC_USER = "jdbc.user"
  val JDBC_PASSWORD = "jdbc.password"
  val SPARK_LOCAL = "spark.local"

  /**
    * 任务相关的常量
    */
  val PARAM_START_DATE = "startDate"
  val PARAM_END_DATE = "endDate"
  val PARAM_START_AGE = "startAge"
  val PARAM_END_AGE = "endage"
  val PARAM_PROFESSIONALS = "professionals"
  val PARAM_CITIES = "cities"
  val PARAM_SEX = "sex"
  val PARAM_KEYWORDS = "keywords"
  val PARAM_CATEGORY_IDS = "categoryIds"

  /**
    * spark作业相关的常量
    */
  val SPARK_APP_NAME = "UserVisitSessionAnalyzeSpark"
  val FIELD_SESSION_ID = "sessionid"
  val FIELD_SEARCH_KEYWORDS = "searchKeywords"
  val FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds"
  val FIELD_AGE = "age"
  val FIELD_PROFESSIONAL = "professional"
  val FIELD_CITY = "city"
  val FIELD_SEX = "sex"
  val FIELD_VISIT_LENGTH = "visitLength"
  val FIELD_STEP_LENGTH = "stepLength"
  val FIELD_START_TIME = "starttime"

  val SESSION_COUNT = "session_count"
  val TIME_PERIOD_1s_3s = "1s_3s"
  val TIME_PERIOD_4s_6s = "4s_6s"
  val TIME_PERIOD_7s_9s = "7s_9s"
  val TIME_PERIOD_10s_30s = "10s_30s"
  val TIME_PERIOD_30s_60s = "30s_60s"
  val TIME_PERIOD_1m_3m = "1m_3m"
  val TIME_PERIOD_3m_10m = "3m_10m"
  val TIME_PERIOD_10m_30m = "10m_30m"
  val TIME_PERIOD_30m = "30m"
  val STEP_PERIOD_1_3 = "1_3"
  val STEP_PERIOD_4_6 = "4_6"
  val STEP_PERIOD_7_9 = "7_9"
  val STEP_PERIOD_10_30 = "10_30"
  val STEP_PERIOD_30_60 = "30_60"
  val STEP_PERIOD_60 = "60"
}
