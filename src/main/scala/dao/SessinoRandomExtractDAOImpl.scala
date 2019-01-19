package dao

import jdbc.JDBCHelper

class SessinoRandomExtractDAOImpl extends ISessionRandomExtractDAO {
  override def insert(sessionRandom: SessionRandomExtract): Unit = {
    val sql =
      """
        |insert into session_random_extract values(?,?,?,?,?)
      """.stripMargin
    val params = Array[AnyRef](
      sessionRandom.taskId,
      sessionRandom.sessionId,
      sessionRandom.startTime,
      sessionRandom.searchKeywords,
      sessionRandom.clickCategoryIds)
    JDBCHelper.executeUpdate(sql, params)
  }
}
