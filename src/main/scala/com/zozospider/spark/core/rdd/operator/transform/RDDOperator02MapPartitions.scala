package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - mapPartitions()
object RDDOperator02MapPartitions {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // mapPartitions():
    // 将待处理的数据以分区为单位发送到计算节点进行处理, 这里的处理是指可以进行任意的处理, 哪怕是过滤数据

    // 可以以分区为单位进行数据转换操作, 但是会将整个分区的数据加载到内存进行引用
    // 如果处理完的数不会释放掉, 在内存少, 数据量大的情况下, 会出现内存溢出, 此时需要考虑 map() 方法

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)

    val rdd2: RDD[Int] = rdd.mapPartitions((iterator: Iterator[Int]) => {
      println(">>>")
      iterator.map(_ * 2)
    })

    rdd2.collect.foreach(println)

    context.stop
  }

}
