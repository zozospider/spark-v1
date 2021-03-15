package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - flatMap()
object RDDOperator04FlatMap {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // flatMap():
    // 将处理的数据进行扁平化后再进行映射处理, 所以算子也称之为扁平映射

    val rdd: RDD[List[Int]] = context.makeRDD(seq = List(List(1, 2), List(3, 4)))

    val rdd2: RDD[Int] = rdd.flatMap((list: List[Int]) => list)

    rdd2.collect.foreach(println)

    context.stop
  }

}
