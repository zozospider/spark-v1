package com.zozospider.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object Sql02UDF3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQL").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // UDF: 自定义求年龄的平均值: 强类型

    val dataFrame: DataFrame = session.read.json("data-dir\\sql\\user.json")
    dataFrame.createOrReplaceTempView("user")

    session.udf.register(name = "myAvgUDF2", functions.udaf(new MyAvgUDF2))
    session.sql("select myAvgUDF2(age) from user").show

    session.close
  }

}

// 自定义 UDF
// IN: 输入的数据类型 Long
// BUF: 缓冲区的数据类型 MyBuff
// OUT: 输出的数据类型 Long
class MyAvgUDF2 extends Aggregator[Long, MyBuff, Long] {

  // z / zero: 初始值或零值
  // 缓冲区的初始化
  override def zero: MyBuff = MyBuff(0L, 0L)

  // 根据输入的数据更新缓冲区的数据
  override def reduce(b: MyBuff, l: Long): MyBuff = {
    b.ageSum = b.ageSum + l
    b.count = b.count + 1
    b
  }

  // 合并缓冲区
  override def merge(b1: MyBuff, b2: MyBuff): MyBuff = {
    b1.ageSum = b1.ageSum + b2.ageSum
    b1.count = b1.count + b2.count
    b1
  }

  // 计算结果
  override def finish(reduction: MyBuff): Long = {
    // ageSum / count
    reduction.ageSum / reduction.count
  }

  // 缓冲区编码操作
  override def bufferEncoder: Encoder[MyBuff] = Encoders.product

  // 输出的编码操作
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong

}

// 自定义缓冲区
case class MyBuff(var ageSum: Long, var count: Long)
