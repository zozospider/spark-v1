package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - coalesce()
object RDDOperator10Coalesce {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // coalesce():
    // 根据数据量缩减分区, 用于大数据集过滤后, 提高小数据集的执行效率
    // 当 spark 程序中存在过多的小任务的时候, 可以通过 coalesce 方法, 收缩合并分区, 减少分区的个数, 减小任务调度成本

    // coalesce() 默认不会将分区的数据打乱重新组合, 可能导致数据不均衡, 出现数据倾斜
    // 如果想要数据均衡, 可以将 coalesce() 的第二个参数 shuffle 设置成 true

    // 缩减分区: coalesce(numPartitions)
    // 缩减分区 & shuffle: coalesce(numPartitions, shuffle = true)

    // val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4, 5, 6), numSlices = 6)
    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4, 5, 6), numSlices = 3)

    // val rdd2: RDD[Int] = rdd.coalesce(numPartitions = 2)
    val rdd2: RDD[Int] = rdd.coalesce(numPartitions = 2, shuffle = true)

    rdd2.saveAsTextFile("output")

    context.stop
  }

}
