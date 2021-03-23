package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - reduce()
object RDDOperator02Reduce {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // reduce():
    // 聚集 RDD 中的所有元素, 先聚合分区内数据, 再聚合分区间数据

    val rdd: RDD[Int] = context.makeRDD(List(10, 2, 3, 4))

    val i: Int = rdd.reduce((i1: Int, i2: Int) => i1 + i2)

    println(i)

    context.stop
  }

}
