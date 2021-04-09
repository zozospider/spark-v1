package com.zozospider.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

// Spark 通过 JDBC 连接数据库: 连接 MariaDB 并写入数据
object Sql03JDBC2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQL").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val context: SparkContext = session.sparkContext
    import session.implicits._

    // 创建表 table_from_spark 并插入两条数据
    val rdd: RDD[TableFromSpark] = context.makeRDD(List(TableFromSpark(10, "s10"), TableFromSpark(20, "s20")))
    val dataSet: Dataset[TableFromSpark] = rdd.toDS
    dataSet.write
      .format("jdbc")
      .option("driver", "org.mariadb.jdbc.Driver")
      .option("url", "jdbc:mysql://111.230.233.137:3306/test")
      .option("user", "zozo")
      .option("password", "xxx")
      .option("dbtable", "table_from_spark")
      .mode(SaveMode.Append)
      .save

    session.close
  }

}

case class TableFromSpark(id: Long, content: String)
