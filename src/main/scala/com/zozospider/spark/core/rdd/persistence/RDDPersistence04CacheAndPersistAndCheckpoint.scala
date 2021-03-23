package com.zozospider.spark.core.rdd.persistence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 持久化 - cache() & persist() & checkpoint()
object RDDPersistence04CacheAndPersistAndCheckpoint {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // cache(): 将数据临时存储在内存中进行数据重用
    // persist(): 将数据临时存储在磁盘文件中进行数据重用, 涉及到磁盘 IO, 性能较低, 但是数据安全, 作业执行完毕后临时文件会被删除
    // checkpoint(): 将数据长久存储在磁盘文件中进行数据重用, 涉及到磁盘 IO, 性能较低, 但是数据安全, 作业执行完毕后临时文件不会被删除
    //               为了保证数据安全, 一般会独立执行作业, 所以会执行多次, 性能更低, 所以一般需要和 cache() 联合使用
    //               建议对 checkpoint() 的 RDD 使用 cache 缓存, 这样 checkpoint 的 job 只需从 cache 缓存中读取数据即可, 否则需要再从头计算一次 RDD

    context.setCheckpointDir("checkpoint")

    val rdd: RDD[String] = context.textFile("data-dir\\word-count")
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => {
      println("***")
      (s, 1)
    })

    // cache() 和 checkpoint() 联合使用
    rdd3.cache
    rdd3.checkpoint

    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rdd4.collect.foreach(println)

    println("------")

    val rdd5: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
    rdd5.collect.foreach(println)

    context.stop
  }

}
