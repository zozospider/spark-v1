package com.zozospider.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCountAll {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("data-dir\\word-count")

    wordCount1(rdd)
    println("------")
    wordCount2(rdd)
    println("------")
    wordCount3(rdd)
    println("------")
    wordCount4(rdd)
    println("------")
    wordCount5(rdd)
    println("------")
    wordCount6(rdd)
    println("------")
    wordCount7(rdd)
    println("------")
    wordCount8(rdd)
    println("------")
    wordCount9(rdd)

    context.stop
  }

  def wordCount1(rdd: RDD[String]): Unit = {
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Iterable[String])] = rdd2.groupBy((s: String) => s)
    val rdd4: RDD[(String, Int)] = rdd3.mapValues((iterable: Iterable[String]) => iterable.size)
    rdd4.collect.foreach(println)
  }

  def wordCount2(rdd: RDD[String]): Unit = {
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    val rdd4: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
    val rdd5: RDD[(String, Int)] = rdd4.mapValues((iterable: Iterable[Int]) => iterable.size)
    rdd5.collect.foreach(println)
  }

  def wordCount3(rdd: RDD[String]): Unit = {
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rdd4.collect.foreach(println)
  }

  def wordCount4(rdd: RDD[String]): Unit = {
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    val rdd4: RDD[(String, Int)] = rdd3.aggregateByKey(zeroValue = 0)(
      seqOp = (i1: Int, i2: Int) => i1 + i2,
      combOp = (i1: Int, i2: Int) => i1 + i2
    )
    rdd4.collect.foreach(println)
  }

  def wordCount5(rdd: RDD[String]): Unit = {
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    val rdd4: RDD[(String, Int)] = rdd3.foldByKey(zeroValue = 0)(
      func = (i1: Int, i2: Int) => i1 + i2
    )
    rdd4.collect.foreach(println)
  }

  def wordCount6(rdd: RDD[String]): Unit = {
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    val rdd4: RDD[(String, Int)] = rdd3.combineByKey(
      createCombiner = (i: Int) => i,
      mergeValue = (i1: Int, i2: Int) => i1 + i2,
      mergeCombiners = (i1: Int, i2: Int) => i1 + i2
    )
    rdd4.collect.foreach(println)
  }

  def wordCount7(rdd: RDD[String]): Unit = {
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((s: String) => (s, 1))
    val map: collection.Map[String, Long] = rdd3.countByKey()
    map.foreach(println)
  }

  def wordCount8(rdd: RDD[String]): Unit = {
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val map: collection.Map[String, Long] = rdd2.countByValue()
    map.foreach(println)
  }

  def wordCount9(rdd: RDD[String]): Unit = {
    val rdd2: RDD[String] = rdd.flatMap((s: String) => s.split(" "))
    val rdd3: RDD[mutable.Map[String, Int]] = rdd2.map((s: String) => mutable.Map[String, Int]((s, 1)))
    val map: mutable.Map[String, Int] = rdd3.reduce((map1: mutable.Map[String, Int], map2: mutable.Map[String, Int]) => {
      map2.foreach((tuple: (String, Int)) => {
        val s: String = tuple._1
        val i: Int = tuple._2
        val l: Int = map1.getOrElse(s, 0) + i
        map1.update(s, l)
      })
      map1
    })
    map.foreach(println)
  }

  def wordCount10(rdd: RDD[String]): Unit = {
    // TODO aggregate & fold
  }

}
