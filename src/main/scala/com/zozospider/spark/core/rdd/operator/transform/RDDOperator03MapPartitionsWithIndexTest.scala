package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - mapPartitionsWithIndex() - test - 获取第二个数据分区的数据
object RDDOperator03MapPartitionsWithIndexTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)

    val rdd2: RDD[Int] = rdd.mapPartitionsWithIndex((index: Int, iterator: Iterator[Int]) => {
      if (index == 1) {
        iterator
      } else {
        Nil.iterator
      }
    })

    rdd2.collect.foreach(println)

    context.stop
  }

}
