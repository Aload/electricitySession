package dao

case class SessionDetail(
                          taskId: Long,
                          userId: Long,
                          sessionId: Long,
                          pageId: Long,
                          actionTime: String,
                          searchKeyword: String,
                          clickCategoryId: Long,
                          clickProductId: Long,
                          orderCategoryIds: String,
                          orderProductIds: String,
                          payCategoryIds: String,
                          payProductIds: String
                        )

trait ISessionDetailDAO extends IDao {
  def insert(sessionDetail: SessionDetail)
}
