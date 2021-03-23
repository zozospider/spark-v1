package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - count()
object RDDOperator04Count {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // count():
    // 返回 RDD 中元素的个数

    val rdd: RDD[Int] = context.makeRDD(List(10, 2, 3, 4))

    val l: Long = rdd.count

    println(l)

    context.stop
  }

}
