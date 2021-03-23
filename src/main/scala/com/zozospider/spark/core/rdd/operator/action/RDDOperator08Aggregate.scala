package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - aggregate()
object RDDOperator08Aggregate {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // aggregate():
    // 分区的数据通过初始值和分区内的数据进行聚合, 然后再和初始值进行分区间的数据聚合

    // aggregateByKey(): zeroValue 只会参与分区内的计算 seqOp
    // aggregate(): zeroValue 不仅参与分区内的计算 seqOp, 还会参与分区间的计算 combOp

    // seqOp:
    //   ((100 + 10) + 2) = 112
    //   ((100 + 3) + 4) = 107
    // combOp:
    //   ((100 + 112) + 107) = 319

    val rdd: RDD[Int] = context.makeRDD(seq = List(10, 2, 3, 4), numSlices = 2)

    val i: Int = rdd.aggregate(zeroValue = 100)(
      seqOp = (i1: Int, i2: Int) => i1 + i2,
      combOp = (i1: Int, i2: Int) => i1 + i2
    )

    println(i)

    context.stop
  }

}
