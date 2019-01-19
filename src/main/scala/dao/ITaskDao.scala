package dao

import domain.Task

trait ITaskDao extends IDao {
  def findById(id: Long): Task
}
