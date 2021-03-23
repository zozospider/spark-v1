package com.zozospider.spark.core.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 序列化 - 闭包检查 - test - 类的方法和属性
object RDD01CheckSerializableTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.makeRDD(List("aa", "bb", "ab", "cc"))

    // 类的构造参数 query 其实就是类的属性
    // A: SearchA 中 filter() 方法直接用到了 query 属性, 所以 query 属性需要序列化, 所以 SearchA 需要序列化 (SearchA 未序列化, 所以会报错)
    // AA: SearchAA 中 filter() 方法直接用到了 query 属性, 所以 query 属性需要序列化, 所以 SearchAA 需要序列化 (SearchAA 有序列化, 所以不会报错)
    // B: SearchB 中 filter() 方法没有直接用到 query 属性, 所以 query 属性不需要序列化, 所以 SearchB 不需要序列化 (所以不会报错)

    // 执行时会报异常: org.apache.spark.SparkException: Task not serializable
    /*val searchA: SearchA = new SearchA(query = "a")
    searchA.getMatch1(rdd).collect.foreach(println)
    println("---")
    searchA.getMatch2(rdd).collect.foreach(println)*/

    println("------")

    // 正常执行
    val searchAA: SearchAA = new SearchAA(query = "a")
    searchAA.getMatch1(rdd).collect.foreach(println)
    println("---")
    searchAA.getMatch2(rdd).collect.foreach(println)

    println("------")

    // 正常执行
    val searchB: SearchB = new SearchB(query = "a")
    searchB.getMatch1(rdd).collect.foreach(println)
    println("---")
    searchB.getMatch2(rdd).collect.foreach(println)

    context.stop
  }

  class SearchA(query: String) {

    def getMatch1(rdd: RDD[String]): RDD[String] = {

      def isMatch(s: String): Boolean = {
        s.contains(query)
      }

      rdd.filter(isMatch)
    }

    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter((s: String) => s.contains(query))
    }

  }

  // case class SearchAA(query: String) {
  class SearchAA(query: String) extends Serializable {

    def getMatch1(rdd: RDD[String]): RDD[String] = {

      def isMatch(s: String): Boolean = {
        s.contains(query)
      }

      rdd.filter(isMatch)
    }

    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter((s: String) => s.contains(query))
    }

  }

  class SearchB(query: String) {

    def getMatch1(rdd: RDD[String]): RDD[String] = {

      def isMatch(s: String, q: String): Boolean = {
        s.contains(q)
      }

      val q: String = query
      rdd.filter(isMatch(_, q))
    }

    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val q: String = query
      rdd.filter((s: String) => s.contains(q))
    }

  }

}
