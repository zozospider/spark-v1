package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - mapPartitionsWithIndex()
object RDDOperator03MapPartitionsWithIndex2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4))

    val rdd2: RDD[(String, String)] = rdd.mapPartitionsWithIndex((index: Int, iterator: Iterator[Int]) => {
      println(">>>")
      // iterator.map((index, _))
      iterator.map((num: Int) => ("index = " + index, "num = " + num))
    })

    rdd2.collect.foreach(println)

    context.stop
  }

}
