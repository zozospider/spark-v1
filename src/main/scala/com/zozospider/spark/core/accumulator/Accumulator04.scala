package com.zozospider.spark.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// 累加器 - 自定义累加器 - WordCount
object Accumulator04 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("data-dir\\word-count")

    val accumulator: WordCountAccumulator = wordCountAccumulator(context)
    rdd.foreach((s: String) => accumulator.add(s))
    println(accumulator.value)

    context.stop
  }

  def wordCountAccumulator(context: SparkContext): WordCountAccumulator = {
    val acc: WordCountAccumulator = new WordCountAccumulator
    context.register(acc)
    acc
  }

}

// 自定义累加器
// IN: word: String
// OUT: Map[word: String, count: Int]
class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

  private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()

  // 累加器是否为初始状态
  override def isZero: Boolean = map.isEmpty

  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new WordCountAccumulator

  // 重置累加器
  override def reset(): Unit = map.clear

  // 向累加器中增加数据
  override def add(v: String): Unit = {
    val array: Array[String] = v.split(" ")
    array.foreach((s: String) => map(s) = map.getOrElse(s, 0) + 1)
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    other.value.foreach((tuple: (String, Int)) => map(tuple._1) = map.getOrElse(tuple._1, 0) + tuple._2)
  }

  // 返回累加器的结果
  override def value: mutable.Map[String, Int] = map

}
