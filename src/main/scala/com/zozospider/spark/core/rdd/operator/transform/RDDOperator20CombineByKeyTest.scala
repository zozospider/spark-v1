package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - combineByKey() - test - 获取相同 key 的 value 的平均值
object RDDOperator20CombineByKeyTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3)), numSlices = 2)

    val rdd2: RDD[(String, (Int, Int))] = rdd.combineByKey(
      createCombiner = (i: Int) => (i, 1),
      mergeValue = (tuple: (Int, Int), i: Int) => (tuple._1 + i, tuple._2 + 1),
      mergeCombiners = (tuple1: (Int, Int), tuple2: (Int, Int)) => (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
    )

    rdd2.collect.foreach(println)

    println("------")

    val rdd3: RDD[(String, Float)] = rdd2.mapValues((tuple: (Int, Int)) => tuple._1.toFloat / tuple._2.toFloat)

    rdd3.collect.foreach(println)

    context.stop
  }

}
