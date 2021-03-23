package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - coalesce() - 扩大分区
object RDDOperator10Coalesce2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 如果想要扩大分区, 则必须将 coalesce() 的第二个参数 shuffle 设置成 true, 否则没有意义 (因为 coalesce() 默认不会将分区的数据打乱重新组合)

    // 缩减分区: coalesce(numPartitions)
    // 缩减分区 & shuffle: coalesce(numPartitions, shuffle = true)

    // 扩大分区 & shuffle: coalesce(numPartitions, shuffle = true)

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4, 5, 6), numSlices = 2)

    val rdd2: RDD[Int] = rdd.coalesce(numPartitions = 3, shuffle = true)

    rdd2.saveAsTextFile("output")

    context.stop
  }

}
