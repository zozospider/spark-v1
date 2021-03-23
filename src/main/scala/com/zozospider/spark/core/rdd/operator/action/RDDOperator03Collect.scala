package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - collect()
object RDDOperator03Collect {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // collect():
    // 在驱动程序中, 以数组 Array 的形式返回数据集的所有元素

    // collect() 方法会将不同的数据按照分区顺序采集到 Driver 端内存中, 形成数组

    val rdd: RDD[Int] = context.makeRDD(List(10, 2, 3, 4))

    val array: Array[Int] = rdd.collect

    println(array.mkString("Array(", ", ", ")"))
    array.foreach(println)

    context.stop
  }

}
