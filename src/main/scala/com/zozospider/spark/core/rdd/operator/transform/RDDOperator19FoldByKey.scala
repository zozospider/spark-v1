package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - foldByKey()
object RDDOperator19FoldByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // foldByKey():
    // 当分区内计算规则和分区间计算规则相同时, aggregateByKey() 就可以简化为 foldByKey()

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3)), numSlices = 2)

    // val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(zeroValue = 0)(math.max, math.max)
    /*val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(zeroValue = 0)(
      seqOp = (i1: Int, i2: Int) => math.max(i1, i2),
      combOp = (i1: Int, i2: Int) => math.max(i1, i2))*/

    // val rdd2: RDD[(String, Int)] = rdd.foldByKey(zeroValue = 0)(math.max)
    val rdd2: RDD[(String, Int)] = rdd.foldByKey(zeroValue = 0)(
      func = (i1: Int, i2: Int) => math.max(i1, i2))

    rdd2.collect.foreach(println)

    context.stop
  }

}
