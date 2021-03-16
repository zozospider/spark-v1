package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - flatMap() - test - 将 List(List(1,2), 3, List(4,5)) 进行扁平化操作
object RDDOperator04FlatMapTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[Any] = context.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val rdd2: RDD[Any] = rdd.flatMap((any: Any) => {
      // 模式匹配
      any match {
        case list: List[_] => list
        case other => List(other)
      }
    })

    rdd2.collect.foreach(println)

    context.stop
  }

}
