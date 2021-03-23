package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - Value 类型 - groupBy()
object RDDOperator06GroupBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // groupBy():
    // 将数据根据指定的规则进行分组, 分区默认不变, 但是数据会被打乱重新组合, 我们将这样的操作称之为 shuffle
    // 极限情况下, 数据可能被分在同一个分区中
    // 一个组的数据在一个分区中, 但是并不是说一个分区中只有一个组

    // groupBy 会将数据源中的每一个数据进行分组, 根据返回的 key 进行分组, 相同的 key 在同一个组中
    // 分组和分区没有必然的关系
    // groupBy 会将数据打乱 (打散), 重新组合, 这个操作我们称之为 shuffle

    val rdd: RDD[Int] = context.makeRDD(seq = List(1, 2, 3, 4), numSlices = 2)

    // 奇数偶数分组
    def f(i: Int): Int = {
      i % 2
    }

    val rdd2: RDD[(Int, Iterable[Int])] = rdd.groupBy(f)

    rdd2.collect.foreach(println)

    context.stop
  }

}
