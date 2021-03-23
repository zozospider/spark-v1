package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - aggregateByKey()
object RDDOperator18AggregateByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // aggregateByKey():
    // 将数据根据不同的规则进行分区内计算和分区间计算

    // aggregateByKey() 存在函数柯里化, 有两个参数列表
    // zeroValue: 初始值, 主要用于碰见第一个 key 时和 value 进行分区计算
    // seqOp: 分区内计算规则
    // combOp: 分区间计算规则

    // ("a", 1), ("b", 2), ("b", 20) | ("a", 10), ("a", 100), ("c", 3)
    // ("a", 1), ("b", 20) | ("a", 100), ("c", 3)
    // ("a", 101), ("b", 20), ("c", 3)

    // ("a", 1), ("b", 2), ("b", 20) | ("a", 10), ("a", 100), ("c", 3)
    // seqOp = (i1: Int, i2: Int) => math.max(i1, i2):
    //   ("a", math.max(0, 1)) = ("a", 1)
    //   ("b", math.max(0, 2)) = ("b", 2) -> ("b", math.max(2, 20)) = ("b", 20)
    //   |
    //   ("a", math.max(0, 10)) = ("a", 10) -> ("a", math.max(10, 100)) = ("a", 100)
    //   ("c", math.max(0, 3)) = ("c", 3)
    // combOp = (i1: Int, i2: Int) => i1 + i2:
    //   ("a", 1 + 100) = ("a", 101)
    //   ("b", 20) = ("b", 20)
    //   ("c", 3) = ("c", 3)

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3)), numSlices = 2)

    // val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(zeroValue = 0)(math.max, _ + _)
    val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(zeroValue = 0)(
      seqOp = (i1: Int, i2: Int) => math.max(i1, i2),
      combOp = (i1: Int, i2: Int) => i1 + i2)

    rdd2.collect.foreach(println)

    context.stop
  }

}
