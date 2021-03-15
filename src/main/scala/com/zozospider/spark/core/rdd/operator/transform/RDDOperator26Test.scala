package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - test - 统计出每一个省份每个广告被点击数量排行的 top3
object RDDOperator26Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 1. 获取原始数据: 时间戳 省份 城市 用户 广告
    val rdd: RDD[String] = context.textFile("data-dir\\agent.log.txt")

    // 2. 结构转换: 时间戳 省份 城市 用户 广告 -> ((省份, 广告), 1)
    val rdd2: RDD[((String, String), Int)] = rdd.map((s: String) => {
      val fields: Array[String] = s.split(" ")
      ((fields(1), fields(4)), 1)
    })

    // 3. 分组聚合: ((省份, 广告), 1) -> ((省份, 广告), sum)
    val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey((i1: Int, i2: Int) => i1 + i2)

    // 4. 结构转换: ((省份, 广告), sum) -> (省份, (广告, sum))
    /*val rdd4: RDD[(String, (String, Int))] = rdd3.map {
      case ((s1: String, s2: String), i: Int) => (s1, (s2, i))
    }*/
    val rdd4: RDD[(String, (String, Int))] = rdd3.map((tuple: ((String, String), Int)) => (tuple._1._1, (tuple._1._2, tuple._2)))

    // 5. 分组: (省份, (广告, sum)) -> (省份, ((广告1, sum), (广告2, sum)))
    val rdd5: RDD[(String, Iterable[(String, Int)])] = rdd4.groupByKey()

    // 6. 排序取前 3 名
    val rdd6: RDD[(String, List[(String, Int)])] = rdd5.mapValues((iterable: Iterable[(String, Int)]) =>
      iterable
        .toList
        // .sortBy((tuple: (String, Int)) => tuple._2)(Ordering.Int.reverse)
        .sortWith((tuple1: (String, Int), tuple2: (String, Int)) => tuple1._2 > tuple2._2)
        .take(3))

    rdd6.collect.foreach(println)

    context.stop
  }

}
