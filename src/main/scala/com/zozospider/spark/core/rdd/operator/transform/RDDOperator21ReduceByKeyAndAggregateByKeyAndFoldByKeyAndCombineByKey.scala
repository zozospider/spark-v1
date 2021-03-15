package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Key - Value 类型 - reduceByKey() aggregateByKey() foldByKey() combineByKey()
object RDDOperator21ReduceByKeyAndAggregateByKeyAndFoldByKeyAndCombineByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // reduceByKey():
    //   相同 key 的第一个数据不进行任何计算, 分区内和分区间计算规则相同
    // aggregateByKey():
    //   相同 key 的第一个数据和初始值进行分区内计算, 分区内和分区间计算规则可以不相同
    // foldByKey():
    //   相同 key 的第一个数据和初始值进行分区内计算, 分区内和分区间计算规则相同
    // combineByKey():
    //   当计算时, 发现数据结构不满足要求时, 可以让第一个数据转换结构, 分区内和分区间计算规则不相同

    // reduceByKey():
    //   调用 combineByKeyWithClassTag()
    // aggregateByKey():
    //   调用 combineByKeyWithClassTag()
    // foldByKey():
    //   调用 combineByKeyWithClassTag()
    // combineByKey():
    //   调用 combineByKeyWithClassTag()

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3)), numSlices = 2)

    val rdd2: RDD[(String, Int)] = rdd.reduceByKey(
      func = (i1: Int, i2: Int) => i1 + i2)

    val rdd3: RDD[(String, Int)] = rdd.aggregateByKey(zeroValue = 0)(
      seqOp = (i1: Int, i2: Int) => i1 + i2,
      combOp = (i1: Int, i2: Int) => i1 + i2)

    val rdd4: RDD[(String, Int)] = rdd.foldByKey(zeroValue = 0)(
      func = (i1: Int, i2: Int) => i1 + i2)

    val rdd5: RDD[(String, Int)] = rdd.combineByKey(
      createCombiner = (i: Int) => i,
      mergeValue = (i1: Int, i2: Int) => i1 + i2,
      mergeCombiners = (i1: Int, i2: Int) => i1 + i2)

    rdd2.collect.foreach(println)
    println("------")
    rdd3.collect.foreach(println)
    println("------")
    rdd4.collect.foreach(println)
    println("------")
    rdd5.collect.foreach(println)

    context.stop
  }

}
