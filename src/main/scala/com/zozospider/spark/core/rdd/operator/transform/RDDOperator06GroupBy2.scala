package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - groupBy()
object RDDOperator06GroupBy2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.makeRDD(seq = List("aa", "ba", "ab", "cc"), numSlices = 2)

    val rdd2: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    rdd2.collect.foreach(println)

    context.stop
  }

}
