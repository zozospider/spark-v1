package com.zozospider.spark.core.rdd.save

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 文件读取与保存
// 相关内容见 RDDOperator11Save.scala
object RDDSave01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // saveAsTextFile():
    // 保存成 Text 文件

    // saveAsObjectFile():
    // 序列化成对象保存到文件
    // 对象文件是将对象序列化后保存的文件, 采用 Java 的序列化机制
    // 可以通过 objectFile[T:ClassTag](path) 函数接收一个路径, 读取对象文件, 返回对应的 RDD
    // 也可以通过调用 saveAsObjectFile() 实现对对象文件的输出. 因为是序列化所以要指定类型

    // saveAsSequenceFile():
    // 保存成 Sequence 文件
    // SequenceFile 文件是 Hadoop 用来存储二进制形式的 key-value 对而设计的一种平面文件 (FlatFile)
    // 在 SparkContext 中, 可以调用 sequenceFile[keyClass, valueClass](path)

    // saveAsSequenceFile() 要求数据格式必须为 (k, v) 类型

    val rdd: RDD[(String, Int)] = context.makeRDD(
      seq = List(("a", 10), ("b", 20), ("b", 20), ("a", 1), ("a", 10), ("c", 3)), numSlices = 2)

    // 保存
    rdd.saveAsTextFile(path = "output")
    rdd.saveAsObjectFile(path = "output1")
    rdd.saveAsSequenceFile(path = "output2")

    // 读取
    val rdd2: RDD[String] = context.textFile("output")
    println(rdd2.collect.mkString("Array(", ", ", ")"))
    println("------")
    val rdd3: RDD[(String, Int)] = context.objectFile[(String, Int)](path = "output1")
    println(rdd3.collect.mkString("Array(", ", ", ")"))
    println("------")
    val rdd4: RDD[(String, Int)] = context.sequenceFile[String, Int](path = "output2")
    println(rdd4.collect.mkString("Array(", ", ", ")"))

    context.stop
  }

}
