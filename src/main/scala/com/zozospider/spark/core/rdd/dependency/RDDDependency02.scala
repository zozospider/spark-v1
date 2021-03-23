package com.zozospider.spark.core.rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 依赖关系
object RDDDependency02 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // OneToOneDependency (extends NarrowDependency(窄依赖)):
    // 窄依赖表示每一个父 (上游) RDD 的 Partition 最多被子 (下游) RDD 的一个 Partition 使用, 窄依赖我们形象的比喻为独生子女

    // ShuffleDependency (宽依赖):
    // 宽依赖表示同一个父 (上游) RDD 的 Partition 被多个子 (下游) RDD 的 Partition 依赖, 会引起 Shuffle
    // 总结: 宽依赖我们形象的比喻为多生

    // OneToOneDependency: 新的 RDD 的一个分区的数据依赖于旧的 RDD 一个分区的数据
    // ShuffleDependency: 新的 RDD 的一个分区的数据依赖于旧的 RDD 多个分区的数据

    val rdd: RDD[String] = context.textFile("data-dir\\word-count")
    println(rdd.dependencies)
    println("------")
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    println(rdd2.dependencies)
    println("------")
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    println(rdd3.dependencies)
    println("------")
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    println(rdd4.dependencies)
    println("------")

    rdd4.collect.foreach(println)

    context.stop
  }

}
