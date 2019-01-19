package dao

case class Top10Session(
                         taskId: Long,
                         categoryId: Long,
                         sessionId: String,
                         clickCount: Long
                       )

trait ITop10SessionDAO extends IDao {
  def insert(top10Session: Top10Session)
}
