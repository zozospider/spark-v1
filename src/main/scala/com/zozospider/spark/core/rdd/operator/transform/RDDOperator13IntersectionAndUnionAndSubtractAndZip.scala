package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - 双 Value 类型 - intersection() union() subtract() zip()
object RDDOperator13IntersectionAndUnionAndSubtractAndZip {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // intersection(), union(), subtract() 要求两个数据源的类型相同
    val rddA: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))
    val rddB: RDD[Int] = context.makeRDD(List(10, 2, 30, 4))

    // intersection():
    // 交集: 对源 RDD 和参数 RDD 求交集后返回一个新的 RDD
    val rdd2: RDD[Int] = rddA.intersection(rddB)
    println(rdd2.collect.mkString("Array(", ", ", ")"))
    println("------")

    // union()
    // 并集: 对源 RDD 和参数 RDD 求并集后返回一个新的 RDD
    val rdd3: RDD[Int] = rddA.union(rddB)
    println(rdd3.collect.mkString("Array(", ", ", ")"))
    println("------")

    // subtract()
    // 差集: 以一个 RDD 元素为主, 去除两个 RDD 中重复元素, 将其他元素保留下来, 求差集
    val rdd4: RDD[Int] = rddA.subtract(rddB)
    val rdd5: RDD[Int] = rddB.subtract(rddA)
    println(rdd4.collect.mkString("Array(", ", ", ")"))
    println(rdd5.collect.mkString("Array(", ", ", ")"))
    println("------")

    // zip()
    // 拉链: 将两个 RDD 中的元素, 以键值对的形式进行合并. 其中, 键值对中的 Key 为第 1 个 RDD 中的元素, Value 为第 2 个 RDD 中的相同位置的元素
    val rdd6: RDD[(Int, Int)] = rddA.zip(rddB)
    println(rdd6.collect.mkString("Array(", ", ", ")"))

    context.stop
  }

}
