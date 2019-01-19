package dao

import jdbc.JDBCHelper

class Top10SessionDAOImpl extends ITop10SessionDAO {
  override def insert(top10Session: Top10Session): Unit = {
    val sql = "insert into top10_session values(?,?,?,?)"
    val params = Array[AnyRef](
      top10Session.taskId,
      top10Session.categoryId,
      top10Session.sessionId,
      top10Session.clickCount)
    JDBCHelper.executeUpdate(sql, params)
  }
}
