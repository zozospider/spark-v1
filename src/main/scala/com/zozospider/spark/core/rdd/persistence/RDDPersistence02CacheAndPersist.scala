package com.zozospider.spark.core.rdd.persistence

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

// 持久化 - cache() & persist()
object RDDPersistence02CacheAndPersist {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // RDD 通过 Cache 或者 Persist 方法将前面的计算结果缓存, 默认情况下会把数据以缓存在 JVM 的堆内存中
    // 但是并不是这两个方法被调用时立即缓存, 而是触发后面的 action 算子时, 该 RDD 将会被缓存在计算节点的内存中, 并供后面重用

    // 缓存有可能丢失, 或者存储于内存的数据由于内存不足而被删除, RDD 的缓存容错机制保证了即使缓存丢失也能保证计算的正确执行
    // 通过基于 RDD 的一系列转换, 丢失的数据会被重算
    // 由于 RDD 的各个 Partition 是相对独立的, 因此只需要计算丢失的部分即可, 并不需要重算全部 Partition

    // Spark 会自动对一些 Shuffle 操作的中间数据做持久化操作 (比如: reduceByKey)
    // 这样做的目的是为了当一个节点 Shuffle 失败了避免重新计算整个输入
    // 但是, 在实际使用的时候, 如果想重用数据, 仍然建议调用 persist 或 cache

    // RDD 对象的持久化操作不一定是为了重用, 在数据执行较长, 或数据比较重要的场合也可以采用持久化操作
    // 保存到磁盘的为临时文件, 作业运行完后会被删除

    val rdd: RDD[String] = context.textFile("data-dir\\word-count")
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => {
      println("***")
      (s, 1)
    })

    // 数据保存到内存中 (1 或 2 个副本)
    rdd3.cache
    // rdd3.persist
    // rdd3.persist(StorageLevel.MEMORY_ONLY)
    // rdd3.persist(StorageLevel.MEMORY_ONLY_2)

    // 数据保存到磁盘中 (1 或 2 个副本)
    // rdd3.persist(StorageLevel.DISK_ONLY)
    // rdd3.persist(StorageLevel.DISK_ONLY_2)

    // 数据保存到内存和磁盘中 (内存不够则溢写磁盘) (1 或 2 个副本)
    // rdd3.persist(StorageLevel.MEMORY_AND_DISK)
    // rdd3.persist(StorageLevel.MEMORY_AND_DISK_2)

    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rdd4.collect.foreach(println)

    println("------")

    val rdd5: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
    rdd5.collect.foreach(println)

    context.stop
  }

}
