package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - map() - test - 从服务器日志数据 apache.log.txt 中获取用户请求 URL 资源路径
object RDDOperator01MapTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("data-dir\\apache.log.txt")

    val rdd2: RDD[String] = rdd.map((s: String) => {
      val arr: Array[String] = s.split(" ")
      arr(6)
    })

    rdd2.collect.foreach(println)

    context.stop
  }

}
