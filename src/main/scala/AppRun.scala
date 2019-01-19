import java.util.{Date, Random}

import com.alibaba.fastjson.{JSON, JSONObject}
import common.Constants
import dao._
import datasource.DataPool
import domain.Task
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import session.SessionAccumulator
import utils.{DateUtils, MathStringUtils, ParamUtils, ValidUtils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object AppRun {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().enableHiveSupport()
      .appName(this.getClass.getSimpleName)
      .master("yarn")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val sqc = sparkSession.sqlContext
    //produce demo data
    DataPool.mock(sc, sqc)
    val taskDao = DaoFactory.getTaskDAO("task").asInstanceOf[TaskDaoImpl]
    //首先查出任务id
    val taskId: Long = ParamUtils.getTaskIdFromArgs(args)
    val task: Task = taskDao.findById(taskId)
    val taskParam: JSONObject = JSON.parseObject(task.taskParam)
    print(taskParam)
    val actionRdd: RDD[Row] = getActionRangeDateRdd(sqc, taskParam)
    //(sessionId,visitAction)
    val sessionRDD: RDD[(String, Row)] = actionRdd.map(x => {
      val sessionId = x.getString(3)
      (sessionId, x)
    })
    //(sessionId,Iterable[VisitAction])
    val groupSessionRDD: RDD[(String, Iterable[Row])] = sessionRDD.groupByKey()
    //(action,partAggrInfo)
    val userSessionRdd: RDD[(Long, String)] = actionSessionRdd(groupSessionRDD)
    //(sessionId,partAggrInfo+userInfo)
    val session2InfoRdd: RDD[(String, String)] = session2AggInfoRdd(sqc, userSessionRdd)
    val sessionAccumulator = new SessionAccumulator
    sc.register(sessionAccumulator)
    //(sessionId,partAggrInfo+userInfo)
    val percolateSessionRdd: RDD[(String, String)] = filterSessionRdd(session2InfoRdd, taskParam, sessionAccumulator)
    //(sessionId,Row[visitAction])
    val session2DetailRdd: RDD[(String, Row)] = getSession2DetailRdd(percolateSessionRdd, sessionRDD)
    //随机抽取
    randomExtractSession(task.taskid, percolateSessionRdd, session2DetailRdd)
    //计算各个范围内的session占比
    calculatePercentSession(sessionAccumulator.value, task.taskid)
  }

  def calculatePercentSession(value: String, taskId: Long): Unit = {

  }

  def randomExtractSession(taskId: Long, percolateSessionRdd: RDD[(String, String)], session2DetailRdd: RDD[(String, Row)]): Unit = {
    val oneHourRdd = percolateSessionRdd.map(info => {
      val aggrInfo = info._2
      val startTime = MathStringUtils.getFieldFromConcatString(aggrInfo, "|", Constants.FIELD_START_TIME)
      val hour = DateUtils.getDateHour(startTime)
      (hour, aggrInfo)
    })
    //每天每个小时的session数量
    val countSessionHour: scala.collection.Map[String, scala.Long] = oneHourRdd.countByKey()
    //使用按时按比例随机抽取算法，计算出每个hour要抽取的session的索引
    //<yyyy-mm-dd_hh,count>=><yyyy-mm-dd,<hh,count>>
    val iterator = countSessionHour.iterator
    val newCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
    val random = new Random
    while (iterator.hasNext) {
      val (key, value) = iterator.next()
      val kList = key.split("_")
      var valueNull: mutable.HashMap[String, Long] = newCountMap.get(kList(0)).asInstanceOf
      if (valueNull == null) {
        valueNull = new mutable.HashMap[String, Long]()
        newCountMap.put(kList(0), valueNull)
      }
      valueNull.put(kList(1), value)
    }
    //取样100  将100个样品平均分配
    val extractNumberPerDay = 100 / newCountMap.size
    //每天每個小時抽取的數據<(yy-mm-dd,(hh,(11,23,4,5)))>
    val tmpHourDayMap = new mutable.HashMap[String, mutable.HashMap[String, ArrayBuffer[Int]]]()

    for ((key, value) <- newCountMap) {
      //计算一天的session数量
      var countDay: Int = 0
      for ((_, y) <- value) {
        countDay = countDay + y.toInt
      }
      var sessionIndexHour: mutable.HashMap[String, ArrayBuffer[Int]] = tmpHourDayMap.get(key).asInstanceOf
      if (null == sessionIndexHour) {
        sessionIndexHour = new mutable.HashMap[String, ArrayBuffer[Int]]()
        tmpHourDayMap.put(key, sessionIndexHour)
      }
      for ((x, y) <- value) {
        //抽样因子
        val mathDivisor: Double = y / countDay
        //每天每个小时需要抽样的个数
        var sampleHourCount = extractNumberPerDay * mathDivisor
        if (sampleHourCount > y) {
          sampleHourCount = y
        }
        var index: ArrayBuffer[Int] = sessionIndexHour.get(x).asInstanceOf
        if (index == null) {
          index = new ArrayBuffer[Int]()
          sessionIndexHour.put(x, index)
        }
        for (_ <- 0 to sampleHourCount) {
          var extractIndex = random.nextInt(y.toInt)
          while (index.contains(extractIndex)) {
            extractIndex = random.nextInt(y.toInt)
          }
          index.append(extractIndex)
        }
      }
    }
    //遍历每天每小时的session，根据随机索引抽取 每个小时存在多条session记录
    //<hour ,row(sessionId,aggrInfo)>
    val groupSessionInfoRdd: RDD[(String, Iterable[String])] = oneHourRdd.groupByKey()
    val indexSessionBufferRdd: RDD[(String, String)] = groupSessionInfoRdd.flatMap(element => {
      val sessionList = new mutable.HashMap[String, String]()
      val date_hour = element._1
      val date = date_hour.split("_")(0)
      val hour = date_hour.split("_")(1)
      //(yy-mm-dd,(hh,(11,23,4,5)))
      val indexBuffer: ArrayBuffer[String] = tmpHourDayMap
        .get(date)
        .asInstanceOf[mutable.HashMap[String, ArrayBuffer[String]]]
        .get(hour)
        .asInstanceOf
      val sessionRandomExtract: ISessionRandomExtractDAO = DaoFactory.getTaskDAO("random").asInstanceOf
      val aggInfoIterator = element._2.iterator
      var pos: Int = 0
      while (aggInfoIterator.hasNext) {
        val value: String = aggInfoIterator.next()
        if (indexBuffer.contains(pos)) {
          val sessionId = MathStringUtils.getFieldFromConcatString(value, "|", Constants.FIELD_SESSION_ID)
          val startTime = MathStringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_START_TIME)
          val keyWord = MathStringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
          val clickCategoryId = MathStringUtils.getFieldFromConcatString(value, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
          sessionRandomExtract.insert(SessionRandomExtract(taskId, sessionId, startTime, keyWord, clickCategoryId))
          sessionList.put(sessionId, sessionId)
        }
        pos = pos + 1
      }
      sessionList
    })
    //(sessionId,sessionId) in (sessionId,Row(visitInfo))=>(String,(String,row))
    indexSessionBufferRdd.join(session2DetailRdd).foreach(item => {
      val rowVisitAction: Row = item._2._2
      val detailDAO: ISessionDetailDAO
      = DaoFactory.getTaskDAO("detail").asInstanceOf
      detailDAO.insert(SessionDetail(
        taskId,
        rowVisitAction.getLong(1),
        rowVisitAction.getLong(2),
        rowVisitAction.getLong(3),
        rowVisitAction.getString(4),
        rowVisitAction.getString(5),
        rowVisitAction.getLong(6),
        rowVisitAction.getLong(7),
        rowVisitAction.getString(8),
        rowVisitAction.getString(9),
        rowVisitAction.getString(10),
        rowVisitAction.getString(11)
      ))
    })
  }

  def getSession2DetailRdd(percolateSessionRdd: RDD[(String, String)],
                           actionRdd: RDD[(String, Row)]): RDD[(String, Row)] = {
    //(sessionId,partAggrInfo+userInfo)(sessionId,Row[visitAction]=>(SessionId,(partAggrInfo+userInfo,row[visitAction]))
    percolateSessionRdd.join(actionRdd).map(f => {
      (f._1, f._2._2)
    })
  }

  def filterSessionRdd(session2InfoRdd: RDD[(String, String)],
                       taskParam: JSONObject, sessionAccumulator: SessionAccumulator): RDD[(String, String)] = {
    val startAge = ParamUtils.getParams(taskParam, Constants.PARAM_END_AGE)
    val endAge = ParamUtils.getParams(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParams(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParams(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParams(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParams(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParams(taskParam, Constants.PARAM_CATEGORY_IDS)
    var _parameter =
      (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|"
      else "") +
        (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|"
        else "") +
        (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|"
        else "") +
        (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|"
        else "") +
        (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|"
        else "") +
        (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|"
        else "") +
        (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds
        else "")
    if (_parameter.endsWith("|")) {
      _parameter = _parameter.substring(0, _parameter.length - 1)
    }
    session2InfoRdd.filter(p => {
      val aggregateInfo = p._2
      judgeParam(aggregateInfo, _parameter, sessionAccumulator)
    })
  }

  def judgeParam(aggregateInfo: String, _parameter: String, sessionAccumulator: SessionAccumulator): Boolean = {
    //按照年纪过滤
    if (!ValidUtils.between(aggregateInfo, Constants.FIELD_AGE,
      _parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) return false
    //按照职业过滤
    if (!ValidUtils.in(aggregateInfo, Constants.FIELD_PROFESSIONAL, _parameter, Constants.PARAM_PROFESSIONALS)) return false
    //按照城市过滤
    if (!ValidUtils.in(aggregateInfo, Constants.FIELD_CITY, _parameter, Constants.PARAM_CITIES)) return false
    //按照性别过滤
    if (!ValidUtils.in(aggregateInfo, Constants.FIELD_SEX, _parameter, Constants.PARAM_SEX)) return false
    //按照搜索词过滤
    if (!ValidUtils.in(aggregateInfo, Constants.FIELD_SEARCH_KEYWORDS, _parameter, Constants.PARAM_KEYWORDS)) return false
    //点击ID
    if (!ValidUtils.in(aggregateInfo, Constants.FIELD_CLICK_CATEGORY_IDS, _parameter, Constants.PARAM_CATEGORY_IDS)) return false
    sessionAccumulator.add(Constants.SESSION_COUNT)
    val visitLen = MathStringUtils.getFieldFromConcatString(aggregateInfo, "|", Constants.FIELD_VISIT_LENGTH).toLong
    val stepLen = MathStringUtils.getFieldFromConcatString(aggregateInfo, "|", Constants.FIELD_STEP_LENGTH).toLong
    calculateVisitLength(visitLen, sessionAccumulator)
    calculateStepLength(stepLen, sessionAccumulator)
    true
  }

  /**
    * 计算访问时长范围
    *
    * @param visitLength
    */
  private def calculateVisitLength(visitLength: Long, sessionAggrAccumulator: SessionAccumulator): Unit = {
    if (visitLength >= 1 && visitLength <= 3) sessionAggrAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength >= 4 && visitLength <= 6) sessionAggrAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength >= 7 && visitLength <= 9) sessionAggrAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength >= 10 && visitLength <= 30) sessionAggrAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    else if (visitLength > 30 && visitLength <= 60) sessionAggrAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength > 60 && visitLength <= 180) sessionAggrAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength > 180 && visitLength <= 600) sessionAggrAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength > 600 && visitLength <= 1800) sessionAggrAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength > 1800) sessionAggrAccumulator.add(Constants.TIME_PERIOD_30m)
  }

  /**
    * 计算访问步长范围
    *
    * @param stepLength
    */
  private def calculateStepLength(stepLength: Long, sessionAggrAccumulator: SessionAccumulator): Unit = {
    if (stepLength >= 1 && stepLength <= 3) sessionAggrAccumulator.add(Constants.STEP_PERIOD_1_3)
    else if (stepLength >= 4 && stepLength <= 6) sessionAggrAccumulator.add(Constants.STEP_PERIOD_4_6)
    else if (stepLength >= 7 && stepLength <= 9) sessionAggrAccumulator.add(Constants.STEP_PERIOD_7_9)
    else if (stepLength >= 10 && stepLength <= 30) sessionAggrAccumulator.add(Constants.STEP_PERIOD_10_30)
    else if (stepLength > 30 && stepLength <= 60) sessionAggrAccumulator.add(Constants.STEP_PERIOD_30_60)
    else if (stepLength > 60) sessionAggrAccumulator.add(Constants.STEP_PERIOD_60)
  }


  /**
    * action 聚合 userInfo
    *
    * @param sqc
    * @param userSessionRdd
    * @return
    */
  private def session2AggInfoRdd(sqc: SQLContext, userSessionRdd: RDD[(Long, String)]) = {
    val sql =
      """
        |select * from user_info
      """.stripMargin
    val userRdd = sqc.sql(sql).rdd
    //(user,row)
    val userInfoRdd = userRdd.map(x => {
      (x.getLong(0), x)
    })
    //根据userID进行聚合  (userId,(partAggrInfo,row))
    val userIdRdd = userSessionRdd.join(userInfoRdd)
    val finalSessionIdRdd = userIdRdd.map(rdd => {
      val partAggrInfo = rdd._2._1
      val rowInfo = rdd._2._2
      val age = rowInfo.getInt(3)
      val professional = rowInfo.getString(4)
      val city = rowInfo.getString(5)
      val sex = rowInfo.getString(6)
      val sessionId = MathStringUtils.getFieldFromConcatString(
        partAggrInfo,
        "|",
        Constants.FIELD_SESSION_ID)
      val fullAggrInfo =
        partAggrInfo +
          "|" + Constants.FIELD_AGE + "=" + age +
          "|" + Constants.FIELD_PROFESSIONAL + "=" + professional +
          "|" + Constants.FIELD_CITY + "=" + city +
          "|" + Constants.FIELD_SEX + "=" + sex
      (sessionId, fullAggrInfo)
    })
    finalSessionIdRdd
  }

  /**
    * 获取session行为的rdd
    *
    * @param groupSessionRDD
    * @return
    */
  private def actionSessionRdd(groupSessionRDD: RDD[(String, Iterable[Row])]) = {
    val userSessionRdd: RDD[(Long, String)] = groupSessionRDD.map(x => {
      val sessionId: String = x._1
      val rowIterator: Iterator[Row] = x._2.iterator
      val sbSearchKeyWord: StringBuffer = StringBuffer
      val sbClickCategory: StringBuffer = StringBuffer
      var userId: Long = null
      //session的起始和结束时间
      var startTime: Date = null
      var endTime: Date = null
      //session的访问步长
      var stepLength: Int = 0
      while (rowIterator.hasNext) {
        val item: Row = rowIterator.next()
        userId = item.getLong(1)
        val searchKeyword: String = item.getString(5)
        val clickCategory: Long = item.getLong(6)

        if (!searchKeyword.isEmpty) {
          if (!sbSearchKeyWord.toString.contains(searchKeyword)) {
            sbSearchKeyWord.append(searchKeyword).append(",")
          }
        }
        if (clickCategory != 0) {
          if (!sbClickCategory.toString.contains(clickCategory)) {
            sbSearchKeyWord.append(clickCategory).append(",")
          }
        }
        val actionTime: String = item.getString(4)
        val actionDate: Date = DateUtils.parseTime(actionTime)
        //计算开始时间和结束时间
        if (startTime == null) startTime = actionDate
        if (endTime == null) endTime = actionDate

        if (actionDate.before(startTime)) startTime = actionDate
        if (actionDate.after(endTime)) endTime = actionDate
        stepLength = stepLength + 1
      }
      val searchKeyWords = MathStringUtils.trimComma(sbSearchKeyWord.toString)
      val clickCategoryIds = MathStringUtils.trimComma(sbClickCategory.toString)
      val visitTime: Long = (endTime.getTime - startTime.getTime) / 1000
      val partAggrInfo =
        Constants.FIELD_SESSION_ID + "=" + sessionId +
          "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords +
          "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds +
          "|" + Constants.FIELD_VISIT_LENGTH + "=" + visitTime +
          "|" + Constants.FIELD_STEP_LENGTH + "=" + stepLength +
          "|" + Constants.FIELD_START_TIME + "=" +
          DateUtils.formatDate(startTime)
      (userId, partAggrInfo)
    })
    userSessionRdd
  }

  /**
    * 获取时间周期内的用户行为
    *
    * @param sqlContext
    * @param taskParam
    * @return
    */
  private def getActionRangeDateRdd(sqlContext: SQLContext, taskParam: JSONObject): RDD[Row] = {
    val startDate = ParamUtils.getParams(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParams(taskParam, Constants.PARAM_END_DATE)
    val sql =
      s"""
         |select * from user_visit_action
         |where date>=$startDate
         |and date <=$endDate
      """.stripMargin
    val actionDf: DataFrame = sqlContext.sql(sql)
    actionDf.rdd
  }
}
