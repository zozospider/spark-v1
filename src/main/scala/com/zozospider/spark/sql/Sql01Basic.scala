package com.zozospider.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Sql01Basic {

  def main(args: Array[String]): Unit = {

    // 创建 SparkSQL 运行环境
    val conf: SparkConf = new SparkConf().setAppName("SQL").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    // type DataFrame = Dataset[Row]
    // 因为 DataFrame 是特定泛型的 DataSet, 所以 DataSet 具备 DataFrame 的所有功能

    // DataFrame
    val dataFrame: DataFrame = session.read.json("data-dir\\sql\\user.json")
    dataFrame.show
    println("***")

    // DataFrame SQL
    dataFrame.createOrReplaceTempView("user")
    session.sql("select * from user").show
    session.sql("select age from user").show
    session.sql("select avg(age) from user").show
    println("***")

    // DataFrame DSL
    dataFrame.select("age", "username").show
    dataFrame.select($"age" + 1).show
    dataFrame.select('age + 1).show

    println("******")

    // DataSet
    val seq: Seq[Int] = Seq(1, 2, 3, 4)
    val dataSet: Dataset[Int] = seq.toDS
    dataSet.show

    // 关闭环境
    session.close
  }

}
