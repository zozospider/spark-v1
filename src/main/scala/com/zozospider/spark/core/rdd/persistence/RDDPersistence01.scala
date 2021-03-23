package com.zozospider.spark.core.rdd.persistence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 持久化
object RDDPersistence01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // RDD 中不存储数据
    // 如果一个 RDD 需要重复使用, 那么需要从头再次执行来获取数据
    // RDD 对象可以重用的, 但是数据无法重用

    val rdd: RDD[String] = context.textFile("data-dir\\word-count")
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => {
      println("***")
      (s, 1)
    })
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rdd4.collect.foreach(println)

    println("------")

    val rdd5: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
    rdd5.collect.foreach(println)

    context.stop
  }

}
