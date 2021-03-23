package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - partitionBy()
// 相关内容见 RDDPartitioner01.scala
object RDDOperator14PartitionBy2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = context.makeRDD(
      seq = List((1, 1), (2, 1), (3, 1), (4, 1)), numSlices = 2)

    // 如果分区器一样, 则返回 RDD 自己
    val rdd2: RDD[(Int, Int)] = rdd.partitionBy(new HashPartitioner(partitions = 2))
    val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new HashPartitioner(partitions = 2))
    println(rdd2 == rdd3)

    rdd3.saveAsTextFile("output")

    context.stop
  }

}
