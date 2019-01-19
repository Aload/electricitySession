package dao

import jdbc.JDBCHelper

class SessionDetailDAOImpl extends ISessionDetailDAO {
  override def insert(sessionDetail: SessionDetail): Unit = {
    val sql =
      """
        |insert into session_detail value(?,?,?,?,?,?,?,?,?,?,?,?)
      """.stripMargin
    val params = Array[AnyRef](
      sessionDetail.taskId,
      sessionDetail.userId,
      sessionDetail.sessionId,
      sessionDetail.pageId,
      sessionDetail.actionTime,
      sessionDetail.searchKeyword,
      sessionDetail.clickCategoryId,
      sessionDetail.clickProductId,
      sessionDetail.orderCategoryIds,
      sessionDetail.orderProductIds,
      sessionDetail.payCategoryIds,
      sessionDetail.payProductIds)

    JDBCHelper.executeUpdate(sql, params)
  }
}
