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
      List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("a", 1000), ("a", 10000), ("c", 3)),
      numSlices = 2)
    // println(rdd.getNumPartitions)

    // 直接分组
    val rddA2: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    // println(rddA2.getNumPartitions)
    rddA2.collect.foreach(println)
    // rddA2.saveAsTextFile("output")

    println("------")

    // key 添加随机前缀
    val random: Random.type = scala.util.Random
    val rddB2: RDD[(String, Int)] = rdd.map((tuple: (String, Int)) => {
      val i: Int = random.nextInt(3)
      (i + "_" + tuple._1, tuple._2)
    })
    // 分组
    val rddB3: RDD[(String, Iterable[Int])] = rddB2.groupByKey()
    // key 删除前缀
    val rddB4: RDD[(String, Iterable[Int])] = rddB3.map((tuple: (String, Iterable[Int])) => {
      val key: String = tuple._1.split("_")(1)
      (key, tuple._2)
    })
    // (String, List) 转换成 (String, Int)
    val rddB5: RDD[(String, Int)] = rddB4.flatMap((tuple: (String, Iterable[Int])) => {
      var list: List[(String, Int)] = Nil
      tuple._2.foreach((i: Int) => {
        list = list :+ (tuple._1, i)
      })
      list
    })
    // 再分组
    val rddB6: RDD[(String, Iterable[Int])] = rddB5.groupByKey()
    // println(rddB6.getNumPartitions)
    rddB6.collect.foreach(println)

    context.stop
  }

}
