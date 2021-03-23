package com.zozospider.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 行动算子 - saveAsTextFile() saveAsObjectFile() saveAsSequenceFile()
object RDDOperator11Save {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // saveAsTextFile():
    // 保存成 Text 文件

    // saveAsObjectFile():
    // 序列化成对象保存到文件

    // saveAsSequenceFile():
    // 保存成 Sequence 文件

    // saveAsSequenceFile() 要求数据格式必须为 (k, v) 类型

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 10), ("b", 20), ("b", 20), ("a", 1), ("a", 10), ("c", 3)), numSlices = 2)

    rdd.saveAsTextFile(path = "output")
    rdd.saveAsObjectFile(path = "output1")
    rdd.saveAsSequenceFile(path = "output2")

    context.stop
  }

}
