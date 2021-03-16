package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - combineByKey()
object RDDOperator20CombineByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // combineByKey():
    // 最通用的对 key-value 型 rdd 进行聚集操作的聚集函数 (aggregation function). 类似于 aggregate()
    // combineByKey() 允许用户返回值的类型与输入不一致

    // createCombiner: 设置初始值, 主要用于碰见第一个 key 时和 value 进行分区计算, 类似 aggregate() 中的 zeroValue

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3)), numSlices = 2)

    val rdd2: RDD[(String, Int)] = rdd.combineByKey(
      createCombiner = (i: Int) => i,
      mergeValue = (i1: Int, i2: Int) => math.max(i1, i2),
      mergeCombiners = (i1: Int, i2: Int) => i1 + i2
    )

    rdd2.collect.foreach(println)

    context.stop
  }

}
