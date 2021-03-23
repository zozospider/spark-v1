package com.zozospider.spark.core.rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 血缘关系
object RDDDependency01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("data-dir\\word-count")
    println(rdd.toDebugString)
    println("------")
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    println(rdd2.toDebugString)
    println("------")
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    println(rdd3.toDebugString)
    println("------")
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    println(rdd4.toDebugString)
    println("------")

    rdd4.collect.foreach(println)

    context.stop
  }

}
