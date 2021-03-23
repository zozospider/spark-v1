package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - sortBy()
object RDDOperator12SortBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // sortBy():
    // 该操作用于排序数据. 在排序之前, 可以将数据通过 f 函数进行处理, 之后按照 f 函数处理的结果进行排序, 默认为升序排列
    // 排序后新产生的 RDD 的分区数与原 RDD 的分区数一致. 中间存在 shuffle 的过程

    // sortBy() 默认不会改变分区, 但是中间存在 shuffle

    val rdd: RDD[Int] = context.makeRDD(seq = List(2, 6, 1, 10, 5, 4), numSlices = 2)

    val rdd2: RDD[Int] = rdd.sortBy((i: Int) => i)

    rdd2.saveAsTextFile("output")

    context.stop
  }

}
