package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - groupByKey()
object RDDOperator16GroupByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // groupByKey():
    // 将数据源的数据根据 key 对 value 进行分组

    val rdd: RDD[(String, Int)] = context.makeRDD(
      List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3)))

    // groupByKey() 和 groupBy() 的区别如下:

    val rdd2: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    rdd2.collect.foreach(println)

    println("------")

    val rdd3: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy((tuple: (String, Int)) => tuple._1)
    rdd3.collect.foreach(println)

    context.stop
  }

}
