package dao

case class SessionRandomExtract(
                                 taskId: Long,
                                 sessionId: String,
                                 startTime: String,
                                 searchKeywords: String,
                                 clickCategoryIds: String)

trait ISessionRandomExtractDAO extends IDao {
  def insert(sessionRandom: SessionRandomExtract)
}
