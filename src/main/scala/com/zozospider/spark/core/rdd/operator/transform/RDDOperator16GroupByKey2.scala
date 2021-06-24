package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

// 转换算子 - Key - Value 类型 - groupByKey() - 双重聚合防止 shuffle 过程中可能的数据倾斜
object RDDOperator16GroupByKey2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    // conf.set("spark.default.parallelism", "4")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = context.makeRDD(
      List(("a", 1), ("a", 10), ("a", 100), ("a", 1000), ("a", 10000), ("b", 2), ("c", 3)),
      numSlices = 2)
    // println(rdd.getNumPartitions)

    // 直接聚合
    val rddA2: RDD[(String, Int)] = rdd.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rddA2.collect.foreach(println)
    // println(rddA2.getNumPartitions)
    // rddA2.saveAsTextFile("output")

    println("------")

    // key 添加随机前缀
    val random: Random.type = scala.util.Random
    val rddB2: RDD[(String, Int)] = rdd.map((tuple: (String, Int)) => {
      val i: Int = random.nextInt(3)
      (i + "_" + tuple._1, tuple._2)
    })
    // 聚合
    val rddB3: RDD[(String, Int)] = rddB2.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    // key 删除前缀
    val rddB4: RDD[(String, Int)] = rddB3.map((tuple: (String, Int)) => {
      val key: String = tuple._1.split("_")(1)
      (key, tuple._2)
    })
    // 再聚合
    val rddB5: RDD[(String, Int)] = rddB4.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    // println(rddB5.getNumPartitions)
    rddB5.collect.foreach(println)

    context.stop
  }

}
