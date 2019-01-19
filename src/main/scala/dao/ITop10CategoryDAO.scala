package dao

case class Top10Category(
                          taskId: Long,
                          categoryId: Long,
                          clickCount: Long,
                          orderCount: Long,
                          payCount: Long
                        )

trait ITop10CategoryDAO extends IDao {
  def insert(top10CategoryDAO: Top10Category)
}
