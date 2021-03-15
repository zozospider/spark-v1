package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

// 算子 - Value 类型 - groupBy() - test - 从服务器日志数据 apache.log 中获取每个时间段访问量
object RDDOperator06GroupByTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("data-dir\\\\apache.log.txt")

    val rdd2: RDD[(String, Int)] = rdd.map((s: String) => {
      val fields: Array[String] = s.split(" ")
      val time: String = fields(3)
      val date: Date = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(time)
      val hour: String = new SimpleDateFormat("HH").format(date)
      (hour, 1)
    })

    val rdd3: RDD[(String, Iterable[(String, Int)])] = rdd2.groupBy(_._1)

    val rdd4: RDD[(String, Int)] = rdd3.map {
      case (hour: String, iterable: Iterable[(String, Int)]) => (hour, iterable.size)
    }

    rdd4.collect.foreach(println)

    context.stop
  }

}
