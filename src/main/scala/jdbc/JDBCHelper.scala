package jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util

import common.Constants
import conf.ConfigurationManager


object JDBCHelper {
  private[jdbc] val dataSource = new util.LinkedList[Connection]()
  initHelper()

  private[jdbc] def initHelper(): Unit = {
    ConfigurationManager.initProperties()
    val datasourcesSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE)
    try {
      val driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER)
      Class.forName(driver)
    } catch {
      case e: Exception => print(e.getMessage)
    } finally {
      println("init jdbc driver is end")
    }
    for (i <- 0 to datasourcesSize) {
      val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
      val user = ConfigurationManager.getProperty(Constants.JDBC_USER)
      val password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
      val conn = DriverManager.getConnection(url, user, password)
      dataSource.push(conn)
    }
  }

  def getConnection(): Connection = {
    try {
      while (dataSource.isEmpty) {
        Thread.sleep(10)
      }
    } catch {
      case e: Exception => print(e.getMessage)
    }
    dataSource.poll()
  }

  def executeUpdate(sql: String, params: Array[Object]): Int = {
    var rtn: Int = 0
    var con: Connection = null
    var pstmt: PreparedStatement = null
    try {
      con = getConnection()
      pstmt = con.prepareStatement(sql)
      for (i <- 0 to params.length) {
        pstmt.setObject(i + 1, params(i))
      }
      pstmt.executeUpdate()
    } catch {
      case e: Exception => print(e.getMessage)
    } finally {
      if (null != con) {
        dataSource.push(con)
      }
    }
    rtn
  }

  def executeQuery(sql: String, params: Array[Object], callBack: QueryCallBack): Unit = {
    var con: Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      con = getConnection()
      pstmt = con.prepareStatement(sql)
      for (i <- 0 to params.length) {
        pstmt.setObject(i + 1, params(i))
      }
      rs = pstmt.executeQuery
      callBack.process(rs)
    } catch {
      case e: Exception =>
        print(e.getMessage)
    }
  }

  def executeBatch(sql: String, params: List[Array[Object]]): Array[Int] = {
    var rtn: Array[Int] = null
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try {
      conn = getConnection()
      pstmt = conn.prepareStatement(sql)
      for (p <- 0 to params.length) {
        for (i <- 0 to params(p).length) {
          pstmt.setObject(i + 1, params(i))
        }
        pstmt.addBatch()
      }
      rtn = pstmt.executeBatch()
      conn.commit()
    } catch {
      case exception: Exception => print(exception.getMessage)
    }
    rtn
  }

  trait QueryCallBack {
    def process(set: ResultSet)
  }

}
