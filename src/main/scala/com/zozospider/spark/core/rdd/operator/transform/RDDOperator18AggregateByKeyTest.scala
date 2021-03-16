package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - aggregateByKey() - test - 获取相同 key 的 value 的平均值
object RDDOperator18AggregateByKeyTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // aggregateByKey() 最终返回数据结果应该和初始值 zeroValue 类型一致

    // ("a", 1), ("b", 2), ("b", 20) | ("a", 10), ("a", 100), ("c", 3)
    // ("a", (1, 1)), ("b", (22, 2)) | ("a", (110, 2)), ("c", (3, 1))
    // ("a", (111, 3)), ("b", (22, 2)), ("c", (3, 1))

    // ("a", 1), ("b", 2), ("b", 20) | ("a", 10), ("a", 100), ("c", 3)
    // seqOp = (tuple: (Int, Int), i: Int) => (tuple._1 + i, tuple._2 + 1):
    //   ("a", (0 + 1, 0 + 1)) = ("a", (1, 1))
    //   ("b", (0 + 2, 0 + 1)) = ("b", (2, 1)) -> ("b", (2 + 20, 1 + 1)) = ("b", (22, 2))
    //   |
    //   ("a", (0 + 10, 0 + 1)) = ("a", (10, 1)) -> ("a", (10 + 100, 1 + 1)) = ("a", (110, 2))
    //   ("c", (0 + 3, 0 + 1)) = ("c", (3, 1))
    // combOp = (tuple1: (Int, Int), tuple2: (Int, Int)) => (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2):
    //   ("a", (1 + 110, 1 + 2)) = ("a", (111, 3))
    //   ("b", (22, 2)) = ("b", (22, 2))
    //   ("c", (3, 1)) = ("c", (3, 1))

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3)), numSlices = 2)

    val rdd2: RDD[(String, (Int, Int))] = rdd.aggregateByKey(zeroValue = (0, 0))(
      seqOp = (tuple: (Int, Int), i: Int) => (tuple._1 + i, tuple._2 + 1),
      combOp = (tuple1: (Int, Int), tuple2: (Int, Int)) => (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
    )

    rdd2.collect.foreach(println)

    println("------")

    // ("a", 111.0 / 3.0) = ("a", 37.0)
    // ("b", 22.0 / 2.0) = ("b", 11.0)
    // ("c", 3.0 / 1.0) = ("c", 3.0)

    // val rdd3: RDD[(String, Float)] = rdd2.map((tuple: (String, (Int, Int))) => (tuple._1, tuple._2._1.toFloat / tuple._2._2.toFloat))
    val rdd3: RDD[(String, Float)] = rdd2.mapValues((tuple: (Int, Int)) => tuple._1.toFloat / tuple._2.toFloat)

    rdd3.collect.foreach(println)

    context.stop
  }

}
