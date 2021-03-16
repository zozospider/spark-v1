package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - filter() - test - 从服务器日志数据 apache.log.txt 中获取 2015 年 5 月 17 日的请求路径
object RDDOperator07FilterTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("data-dir\\\\apache.log.txt")

    val rdd2: RDD[String] = rdd.filter((s: String) => {
      val files: Array[String] = s.split(" ")
      val time: String = files(3)
      time.startsWith("17/05/2015")
    })

    rdd2.collect.foreach(println)

    context.stop
  }

}
