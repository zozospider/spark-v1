package com.zozospider.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

// 转换算子 - Key - Value 类型 - partitionBy()
object RDDOperator14PartitionBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // partitionBy():
    // 将数据按照指定 Partitioner 重新进行分区
    // Spark 默认的分区器是 HashPartitioner, 其他可用分区器有 RangePartitioner 等, 自定义分区器继承 Partitioner 即可

    // 根据指定规则对数据重新进行分区

    val rdd: RDD[(Int, Int)] = context.makeRDD(
      seq = List((1, 1), (2, 1), (3, 1), (4, 1)), numSlices = 2)

    // RDD -> PairRDDFunctions: 隐式转换 (二次编译)
    // object RDD {
    //   implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    //      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    //      new PairRDDFunctions(rdd)
    //   }
    // }
    val rdd2: RDD[(Int, Int)] = rdd.partitionBy(new HashPartitioner(partitions = 2))

    rdd2.saveAsTextFile("output")

    context.stop
  }

}
