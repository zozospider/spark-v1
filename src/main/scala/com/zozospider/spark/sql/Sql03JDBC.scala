package com.zozospider.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

// Spark 通过 JDBC 连接数据库: 连接 MariaDB 并读取数据
object Sql03JDBC {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQL").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 通用的 load 方法读取
    // 注意: url 需要使用 mysql 的 url, 参考: https://stackoverflow.com/questions/52718788/how-to-read-data-from-mariadb-using-spark-java
    val dataFrame: DataFrame = session.read
      .format("jdbc")
      .option("driver", "org.mariadb.jdbc.Driver")
      .option("url", "jdbc:mysql://111.230.233.137:3306/test")
      .option("user", "zozo")
      .option("password", "xxx")
      .option("dbtable", "teacher")
      .load
    dataFrame.show

    session.close
  }

}
