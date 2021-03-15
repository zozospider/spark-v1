package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Key - Value 类型 - leftOuterJoin() rightOuterJoin()
object RDDOperator24LeftOuterJoinAndRightOuterJoin {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // leftOuterJoin() rightOuterJoin():
    // 类似于 SQL 的左外连接, 右外连接

    val rddA: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("c", 3)))
    val rddB: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 100), ("b", 200), ("a", 1000), ("d", 400)))

    val rdd2: RDD[(String, (Int, Option[Int]))] = rddA.leftOuterJoin(rddB)
    val rdd3: RDD[(String, (Option[Int], Int))] = rddB.rightOuterJoin(rddA)

    val rdd4: RDD[(String, (Int, Option[Int]))] = rddB.leftOuterJoin(rddA)
    val rdd5: RDD[(String, (Option[Int], Int))] = rddA.rightOuterJoin(rddB)

    rdd2.collect.foreach(println)
    println("---")
    rdd3.collect.foreach(println)
    println("------")
    rdd4.collect.foreach(println)
    println("---")
    rdd5.collect.foreach(println)

    context.stop
  }

}
