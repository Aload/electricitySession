package dao

import jdbc.JDBCHelper

class Top10CategoryDAOImpl extends ITop10CategoryDAO {
  override def insert(category: Top10Category): Unit = {
    val sql = "insert into top10_category values(?,?,?,?,?)"
    val params = Array[AnyRef](
      category.taskId,
      category.categoryId,
      category.clickCount,
      category.orderCount,
      category.payCount)
    JDBCHelper.executeUpdate(sql, params)
  }
}
