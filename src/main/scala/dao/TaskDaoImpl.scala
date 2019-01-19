package dao

import java.sql.ResultSet

import domain.Task
import jdbc.JDBCHelper
import jdbc.JDBCHelper.QueryCallBack

class TaskDaoImpl extends ITaskDao {

  def findById(id: Long): Task = {
    val task = new Task
    val sql =
      """
        |select * from task where task_id=?
      """.stripMargin
    val param = Array[Object](id)
    val jdbcHelper = JDBCHelper
    jdbcHelper.executeQuery(sql, param, new QueryCallBack {
      override def process(rs: ResultSet): Unit = {
        if (rs.next()) {
          val taskid = rs.getLong(1)
          val taskName = rs.getString(2)
          val createTime = rs.getString(3)
          val startTime = rs.getString(4)
          val finishTime = rs.getString(5)
          val taskType = rs.getString(6)
          val taskStatus = rs.getString(7)
          val taskParam = rs.getString(8)

          task.taskid = taskid
          task.taskName = taskName
          task.createTime = createTime
          task.startTime = startTime
          task.finishTime = finishTime
          task.taskType = taskType
          task.taskStatus = taskStatus
          task.taskParam = taskParam
        }
      }
    })
    task
  }
}
