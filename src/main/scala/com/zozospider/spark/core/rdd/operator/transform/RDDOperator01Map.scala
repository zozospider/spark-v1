package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - map()
object RDDOperator01Map {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // map():
    // 将处理的数据逐条进行映射转换, 这里的转换可以是类型的转换, 也可以是值的转换

    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))

    /*def f(i: Int): Int = {
      i * 2
    }
    val rdd2: RDD[Int] = rdd.map(f)*/
    // val rdd2: RDD[Int] = rdd.map((i: Int) => i * 2)
    // val rdd2: RDD[Int] = rdd.map((i: Int) => i * 2)
    // val rdd2: RDD[Int] = rdd.map(i => i * 2)
    val rdd2: RDD[Int] = rdd.map(_ * 2)

    rdd2.collect.foreach(println)

    context.stop
  }

}
