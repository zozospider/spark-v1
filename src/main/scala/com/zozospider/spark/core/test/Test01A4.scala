package com.zozospider.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// 需求 A: Top10 热门品类 - 实现方式 4
object Test014 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 读取原始日志
    val rdd: RDD[String] = context.textFile("data-dir\\test\\user_visit_action_simple.txt")

    // 累加器
    val accumulator: Test01AAccumulator = test01AAccumulator(context)
    rdd.foreach((s: String) => {
      val fields: Array[String] = s.split("_")
      if (fields(6) != "-1") {
        // 点击
        accumulator.add(fields(6), "A")
      } else if (fields(8) != "null") {
        // 下单
        val array: Array[String] = fields(8).split(",")
        array.foreach((s: String) => accumulator.add(s, "B"))
      } else if (fields(10) != "null") {
        // 支付
        val array: Array[String] = fields(10).split(",")
        array.foreach((s: String) => accumulator.add(s, "C"))
      }
    })
    println(accumulator.value)
    println("------")

    // 将品类排序 (按点击数, 下单数, 支付数排序), 取前 10 名, 采集并打印
    val list: List[(String, CategoryCount)] = accumulator.value
      .toList
      .sortWith((tuple1: (String, CategoryCount), tuple2: (String, CategoryCount)) => {
        if (tuple1._2.aCount > tuple2._2.aCount) {
          true
        } else if (tuple1._2.aCount == tuple2._2.aCount) {
          if (tuple1._2.bCount > tuple2._2.bCount) {
            true
          } else if (tuple1._2.bCount == tuple2._2.bCount) {
            tuple1._2.cCount >= tuple2._2.cCount
          } else {
            false
          }
        } else {
          false
        }
      })
      .take(10)
    list.foreach(println)

    context.stop
  }

  def test01AAccumulator(context: SparkContext): Test01AAccumulator = {
    val acc: Test01AAccumulator = new Test01AAccumulator
    context.register(acc)
    acc
  }

}

case class CategoryCount(var aCount: Int, var bCount: Int, var cCount: Int)

// 自定义累加器
// IN: (品类 ID: String, 行为类型: String)
// OUT: Map[品类 ID: String, 行为数: CategoryCount]
class Test01AAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, CategoryCount]] {

  private val map: mutable.Map[String, CategoryCount] = mutable.Map[String, CategoryCount]()

  // 累加器是否为初始状态
  override def isZero: Boolean = map.isEmpty

  // 复制累加器
  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, CategoryCount]] = new Test01AAccumulator

  // 重置累加器
  override def reset(): Unit = map.clear

  // 向累加器中增加数据
  override def add(v: (String, String)): Unit = {
    val categoryCount: CategoryCount = map.getOrElse(v._1, CategoryCount(0, 0, 0))
    if (v._2 == "A") {
      categoryCount.aCount += 1
    } else if (v._2 == "B") {
      categoryCount.bCount += 1
    } else if (v._2 == "C") {
      categoryCount.cCount += 1
    }
    map(v._1) = categoryCount
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, CategoryCount]]): Unit = {
    other.value.foreach((tuple: (String, CategoryCount)) => {
      val categoryCount: CategoryCount = map.getOrElse(tuple._1, CategoryCount(0, 0, 0))
      categoryCount.aCount += tuple._2.aCount
      categoryCount.bCount += tuple._2.bCount
      categoryCount.cCount += tuple._2.cCount
      map(tuple._1) = categoryCount
    })
  }

  // 返回累加器的结果
  override def value: mutable.Map[String, CategoryCount] = map

}
