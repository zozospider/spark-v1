package com.zozospider.spark.streaming.test

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}


object MyDbUtil {

  val dataSource: HikariDataSource = init()

  def init(): HikariDataSource = {
    val hikariConfig: HikariConfig = new HikariConfig()
    hikariConfig.setMaximumPoolSize(20)
    hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver")
    hikariConfig.setJdbcUrl("jdbc:mysql://111.230.233.137:3306/test")
    hikariConfig.setUsername("zozo")
    hikariConfig.setPassword("zzloveooforever520")
    // hikariConfig.setAutoCommit(false)
    new HikariDataSource(hikariConfig)
  }

  def getConnection: Connection = {
    dataSource.getConnection
  }

  // 记录是否存在
  def isExist(sql: String, params: Array[Any]): Boolean = {
    var boolean: Boolean = false
    var ps: PreparedStatement = null
    var connection: Connection = null
    try {
      connection = getConnection
      ps = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          ps.setObject(i + 1, params(i))
        }
      }
      val rs: ResultSet = ps.executeQuery()
      boolean = rs.next()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(ps, connection)
    }
    boolean
  }

  // 查询字段
  def get[T](sql: String, params: Array[Any]): T = {
    var any: Any = null
    var ps: PreparedStatement = null
    var connection: Connection = null
    try {
      connection = getConnection
      ps = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          ps.setObject(i + 1, params(i))
        }
      }
      val rs: ResultSet = ps.executeQuery()
      if (rs.next()) {
        // long = rs.getLong(1)
        any = rs.getObject(1)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(ps, connection)
    }
    any.asInstanceOf[T]
  }

  // 插入一条数据
  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var int: Int = -1
    var ps: PreparedStatement = null
    var connection: Connection = null
    try {
      connection = getConnection
      connection.setAutoCommit(false)
      ps = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          ps.setObject(i + 1, params(i))
        }
      }
      int = ps.executeUpdate()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close(ps, connection)
    }
    int
  }

  def close(ps: PreparedStatement, connection: Connection): Unit = {
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val bool: Boolean = isExist("select * from teacher where tid = ?", Array("02"))
    val s: String = get[String]("select tname from teacher where tid = ?", Array("02"))
    val time: Timestamp = get[Timestamp]("select Sage from student where sid = ?", Array("03"))
    println(bool)
    println(s)
    println(time)
  }

}
