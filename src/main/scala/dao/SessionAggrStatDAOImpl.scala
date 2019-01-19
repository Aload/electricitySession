package dao

import jdbc.JDBCHelper

class SessionAggrStatDAOImpl extends ISessionAggrStatDAO {
  override def insert(sessAgg: SessionAggrStat): Unit = {
    val sql =
      """
        |insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      """.stripMargin
    val params = Array[AnyRef](
      sessAgg.taskid,
      sessAgg.session_count,
      sessAgg.visit_length_1s_3s_ratio,
      sessAgg.visit_length_4s_6s_ratio,
      sessAgg.visit_length_7s_9s_ratio,
      sessAgg.visit_length_10s_30s_ratio,
      sessAgg.visit_length_30s_60s_ratio,
      sessAgg.visit_length_1m_3m_ratio,
      sessAgg.visit_length_3m_10m_ratio,
      sessAgg.visit_length_10m_30m_ratio,
      sessAgg.visit_length_30m_ratio,
      sessAgg.step_length_1_3_ratio,
      sessAgg.step_length_4_6_ratio,
      sessAgg.step_length_7_9_ratio,
      sessAgg.step_length_10_30_ratio,
      sessAgg.step_length_30_60_ratio,
      sessAgg.step_length_60_ratio)
    JDBCHelper.executeUpdate(sql, params)
  }
}
