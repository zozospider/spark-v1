package com.zozospider.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    // 建立和 Spark Framework 的连接, 创建 Application
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
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

    // 3. 通过单词分组
    // "Hello" -> ("Hello", "Hello", "Hello")
    // "Spark" -> ("Spark")
    // "World" -> ("World", "World")
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy((s: String) => s)
    println("wordGroup:")
    wordGroup.foreach((tuple: (String, Iterable[String])) => println(s"${tuple._1} -> ${tuple._2}"))
    println("------")

    // 4. 每个元素变成单词与数据量和的元组
    // ("Hello", 3)
    // ("Spark", 1)
    // ("World", 2)
    val wordCount: RDD[(String, Int)] = wordGroup.mapValues((iterable: Iterable[String]) => iterable.size)
    // val wordCount: RDD[(String, Int)] = wordGroup.map((tuple: (String, Iterable[String])) => (tuple._1, tuple._2.size))
    /*
    val wordCount: RDD[(String, Int)] = wordGroup.map {
      case (s, list) => (s, list.size)
    }
    */
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
