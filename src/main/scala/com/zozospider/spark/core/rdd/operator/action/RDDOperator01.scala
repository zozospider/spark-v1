package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子
object RDDOperator01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 行动算子会触发 Job (作业) 的执行
    // 底层代码调用的是环境对象的 runJob 方法
    // 底层代码中会创建 ActiveJob, 并提交执行

    val rdd: RDD[Int] = context.makeRDD(List(10, 2, 3, 4))

    rdd.collect

    context.stop
  }

}
