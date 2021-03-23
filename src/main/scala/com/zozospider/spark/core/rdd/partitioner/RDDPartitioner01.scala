package com.zozospider.spark.core.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

// 分区器
// 相关内容见 RDDOperator14PartitionBy.scala, RDDOperator14PartitionBy2.scala
object RDDPartitioner01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // Spark 目前支持 Hash 分区和 Range 分区, 和用户自定义分区, Hash 分区为当前的默认分区
    // 分区器直接决定了 RDD 中分区的个数, RDD 中每条数据经过 Shuffle 后进入哪个分区, 进而决定了 Reduce 的个数
    // 只有 Key-Value 类型的 RDD 才有分区器, 非 Key-Value 类型的 RDD 分区的值是 None
    // 每个 RDD 的分区 ID 范围: 0 ~ (numPartitions - 1), 决定这个值是属于那个分区的

    // HashPartitioner: 对于给定的 key, 计算其 hashCode, 并除以分区个数取余
    // RangePartitioner: 将一定范围内的数据映射到一个分区中, 尽量保证每个分区数据均匀, 而且分区间有序
    // 自定义分区器: 继承 Partitioner 并实现方法

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 1), ("b", 2), ("b", 20), ("a", 10), ("a", 100), ("c", 3), ("d", 4)), numSlices = 2)

    val rdd2: RDD[(String, Int)] = rdd.partitionBy(new MyPartitioner(partitions = 4))

    rdd2.saveAsTextFile("output")

    context.stop
  }

}

// 自定义分区器
class MyPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  // 分区数量
  override def numPartitions: Int = partitions

  // 根据数据的 key 值返回数据的分区索引 (从 0 开始)
  override def getPartition(key: Any): Int = {
    key match {
      case "a" => 1
      case "b" => 2
      case "c" => 3
      case _ => 0
    }
  }

}
