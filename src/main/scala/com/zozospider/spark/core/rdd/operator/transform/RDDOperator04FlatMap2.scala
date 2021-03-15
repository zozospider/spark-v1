package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - flatMap()
object RDDOperator04FlatMap2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.makeRDD(seq = List("aa bb", "ab aa"))

    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))

    rdd2.collect.foreach(println)

    context.stop
  }

}
