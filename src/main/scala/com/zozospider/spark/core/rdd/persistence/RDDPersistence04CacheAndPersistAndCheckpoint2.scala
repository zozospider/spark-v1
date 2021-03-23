package com.zozospider.spark.core.rdd.persistence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 持久化 - cache() & persist() & checkpoint()
object RDDPersistence04CacheAndPersistAndCheckpoint2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // Cache 缓存只是将数据保存起来, 不切断血缘依赖

    context.setCheckpointDir("checkpoint")

    val rdd: RDD[String] = context.textFile("data-dir\\word-count")
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))

    // cache(): 会在血缘关系中添加新的依赖, 一旦出现问题, 可以重头读取数据
    // rdd3.cache

    // checkpoint(): 执行过程中, 会切断血缘关系, 重新建立新的血缘关系, 等同于改变数据源
    rdd3.checkpoint

    println(rdd3.toDebugString)
    rdd3.collect
    println("------")
    println(rdd3.toDebugString)

    context.stop
  }

}
