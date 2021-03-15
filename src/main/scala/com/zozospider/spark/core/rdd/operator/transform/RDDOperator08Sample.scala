package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - sample()
object RDDOperator08Sample {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // sample():
    // 根据指定的规则从数据集中抽取数据

    // 应用: 数据倾斜时 (如 groupBy 的 shuffle 导致分区数据不均衡) 抽取数据判断某些数据是否过多, 考虑转移

    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // withReplacement: 抽取后的数据是否放回 true (放回), false (不放回)
    // fraction: 如果抽取不放回, 表示数据源中每条数据被抽取的概率 (基准值的概念)
    //           如果抽取放回, 表示数据源中的每条数据被抽取的可能次数
    // seed: 抽取数据时随机算法的种子, 如果不传递, 那么使用当前系统时间
    // val rdd2: RDD[Int] = rdd.sample(withReplacement = false, fraction = 0.4, seed = 1)
    // val rdd2: RDD[Int] = rdd.sample(withReplacement = false, fraction = 0.4)
    val rdd2: RDD[Int] = rdd.sample(withReplacement = true, fraction = 2)

    println(rdd2.collect.mkString("Array(", ", ", ")"))

    context.stop
  }

}
