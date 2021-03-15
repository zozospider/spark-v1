package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 算子 - Value 类型 - distinct()
object RDDOperator09Distinct {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // distinct():
    // 将数据集中重复的数据去重

    // 去重逻辑在 RDD.distinct() 中:
    // case _ => map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)

    //    4
    //    2
    //    1
    //    3
    //    3
    //    2

    //    .map(x => (x, null))
    // -> 3 -> null
    //    2 -> null
    //    2 -> null
    //    1 -> null
    //    3 -> null
    //    4 -> null

    //    .reduceByKey((x, _) => x, numPartitions)
    // -> 4 -> null
    //    1 -> null
    //    2 -> null
    //    3 -> null

    //    .map(_._1)
    // -> 2
    //    4
    //    3
    //    1

    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 2, 3, 4))

    val rdd2: RDD[Int] = rdd.distinct

    rdd2.collect.foreach(println)

    context.stop
  }

}
