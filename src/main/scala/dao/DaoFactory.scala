package dao

object DaoFactory {

  def getTaskDAO(types: String): IDao = {
    types match {
      case "task" => new TaskDaoImpl
      case "aggr" => new SessionAggrStatDAOImpl
      case "Random" => new SessinoRandomExtractDAOImpl
      case "detail" => new SessionDetailDAOImpl
      case "top_category" => new Top10CategoryDAOImpl
      case "top_session" => new Top10SessionDAOImpl
    }
  }
}
