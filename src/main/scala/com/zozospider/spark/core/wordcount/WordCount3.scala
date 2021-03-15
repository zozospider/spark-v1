package com.zozospider.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount3 {

  def main(args: Array[String]): Unit = {

    // 建立和 Spark Framework 的连接, 创建 Application
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    val context: SparkContext = new SparkContext(conf)

    // 执行业务操作
    // 1. 读取文件, 获取一行一行的数据
    // "Hello Spark"
    // "Hello World"
    // "World Hello"
    val lines: RDD[String] = context.textFile("data-dir\\word-count")
    println("lines:")
    lines.foreach(println)
    println("------")

    // 2. 将每行数据进行拆分, 形成一个个单词
    // "Hello"
    // "Spark"
    // "Hello"
    // "World"
    // "World"
    // "Hello"
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println("words:")
    lines.foreach(println)
    println("------")

    // 3. 每个单词变成单词与数据量 1 的元组
    // ("Hello", 1)
    // ("Spark", 1)
    // ("Hello", 1)
    // ("World", 1)
    // ("World", 1)
    // ("Hello", 1)
    val wordToOne: RDD[(String, Int)] = words.map((s: String) => (s, 1))
    println("wordToOne:")
    // wordToOne.foreach((tuple: (String, Int)) => println(s"${tuple._1} -> ${tuple._2}"))
    wordToOne.foreach(println)
    println("------")

    // 4. Spark 提供的 reduceByKey 可以对相同 key 数据的 value 进行 reduce 聚合
    // ("Hello", 3)
    // ("Spark", 1)
    // ("World", 2)
    // val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println("wordCount:")
    wordCount.foreach(println)
    println("------")

    // 5. 采集并打印结果
    val wordCountArray: Array[(String, Int)] = wordCount.collect
    println("wordCountArray:")
    wordCountArray.foreach(println)
    println("------")

    // 关闭连接
    context.stop
  }

}
