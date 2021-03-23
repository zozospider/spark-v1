package com.zozospider.spark.core.rdd.persistence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 持久化 - checkpoint()
object RDDPersistence03Checkpoint {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 所谓的检查点其实就是通过将 RDD 中间结果写入磁盘
    // 由于血缘依赖过长会造成容错成本过高, 这样就不如在中间阶段做检查点容错, 如果检查点之后有节点出现问题, 可以从检查点开始重做血缘, 减少了开销
    // 对 RDD 进行 checkpoint 操作并不会马上被执行, 必须执行 Action 操作才能触发

    // checkpoint() 为了保证数据安全, 一般会独立执行作业, 所以会执行多次, 性能更低, 解决方案见 RDDPersistence04CacheAndPersistAndCheckpoint.scala

    // checkpoint 需要指定检查点的保存路径 (一般在分布式存储系统中如 HDFS), 作业运行完后不会被删除
    context.setCheckpointDir("checkpoint")

    val rdd: RDD[String] = context.textFile("data-dir\\word-count")
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => {
      println("***")
      (s, 1)
    })

    // 数据保存到 checkpoint 指定路径中
    rdd3.checkpoint

    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rdd4.collect.foreach(println)

    println("------")

    val rdd5: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
    rdd5.collect.foreach(println)

    context.stop
  }

}
