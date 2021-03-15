package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - sortBy()
object RDDOperator12SortBy2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = context.makeRDD(seq = List(("a", 1), ("c", 3), ("b", 2)), numSlices = 2)

    // sortBy() 的第二个参数 ascending 为排序方式: true (升序, 默认), false (降序)

    // val rdd2: RDD[(String, Int)] = rdd.sortBy(f = (tuple: (String, Int)) => tuple._1)
    val rdd2: RDD[(String, Int)] = rdd.sortBy(f = (tuple: (String, Int)) => tuple._1, ascending = false)

    rdd2.collect.foreach(println)

    context.stop
  }

}
