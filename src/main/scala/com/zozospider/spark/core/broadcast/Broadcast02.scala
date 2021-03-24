package com.zozospider.spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// 广播变量
object Broadcast02 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Broadcast").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 广播变量: 分布式共享只读变量

    // 广播变量用来高效分发较大的对象, 向所有工作节点发送一个较大的只读值, 以供一个或多个 Spark 操作使用
    // 比如, 如果你的应用需要向所有节点发送一个较大的只读查询表, 广播变量用起来都很顺手
    // 在多个并行操作中使用同一个变量, 但是 Spark 会为每个任务分别发送

    // 如果 map 数据量很大, 一个 Executor 中又有大量 Task, 可能会造成每个 Task 都有 map 的备份, 造成大量冗余

    // Executor 其实就一个 JVM, 所以在启动时, 会自动分配内存
    // 完全可以将任务中的闭包数据放置在 Executor 的内存中, 达到共享的目的
    // Spark 中的广播变量就可以将闭包的数据保存到 Executor 的内存中
    // Spark 中的广播变量不能够更改

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("c", 3)))
    val map: mutable.Map[String, Int] = mutable.Map(("a", 10), ("b", 20), ("c", 30))

    // 创建广播变量
    val broadcast: Broadcast[mutable.Map[String, Int]] = context.broadcast(map)

    val rdd2: RDD[(String, (Int, Int))] = rdd.map((tuple: (String, Int)) =>
      // 访问广播变量
      (tuple._1, (tuple._2, broadcast.value.getOrElse(tuple._1, 0))))

    rdd2.collect.foreach(println)

    context.stop
  }

}
