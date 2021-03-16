package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - first()
object RDDOperator05First {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // first():
    // 返回 RDD 中的第一个元素

    val rdd: RDD[Int] = context.makeRDD(List(10, 2, 3, 4))

    val i: Int = rdd.first

    println(i)

    context.stop
  }

}
