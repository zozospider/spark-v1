package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - fold()
object RDDOperator09Fold {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // fold():
    // 折叠操作, aggregate 的简化版操作

    // 当分区内计算规则和分区间计算规则相同时, aggregate() 就可以简化为 fold()

    val rdd: RDD[Int] = context.makeRDD(seq = List(10, 2, 3, 4), numSlices = 2)

    val i: Int = rdd.fold(zeroValue = 100)(
      op = (i1: Int, i2: Int) => i1 + i2)

    println(i)

    context.stop
  }

}
