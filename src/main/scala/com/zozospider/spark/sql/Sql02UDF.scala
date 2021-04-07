package com.zozospider.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Sql02UDF {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQL").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 用户可以通过 SparkSession.udf 功能添加自定义函数, 实现自定义功能

    val dataFrame: DataFrame = session.read.json("data-dir\\sql\\user.json")
    dataFrame.createOrReplaceTempView("user")
    session.sql("select username, age from user").show

    session.udf.register(name = "prefixName", func = (s: String) => {
      "Name: " + s
    })
    session.sql("select prefixName(username), age from user").show

    session.close
  }

}
