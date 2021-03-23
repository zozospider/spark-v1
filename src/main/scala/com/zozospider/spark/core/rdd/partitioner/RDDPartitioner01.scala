package com.zozospider.spark.core.rdd.partitioner

import org.apache.spark.{SparkConf, SparkContext}

// 分区器
object RDDPartitioner01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)


  }

}
