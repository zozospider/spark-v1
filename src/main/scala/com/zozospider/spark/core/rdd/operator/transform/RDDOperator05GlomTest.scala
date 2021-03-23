package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - glom() - test - 计算所有分区最大值求和 (分区内取最大值, 分区间最大值求和)
object RDDOperator05GlomTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)

    val rdd2: RDD[Array[Int]] = rdd.glom()

    val rdd3: RDD[Int] = rdd2.map((array: Array[Int]) => array.max)

    val array: Array[Int] = rdd3.collect
    array.foreach(println)
    println("---")
    println(array.sum)

    context.stop
  }

}
