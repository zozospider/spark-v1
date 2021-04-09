package com.zozospider.spark.streaming.dstream.create

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// DStream 创建 - RDD 队列
object DStreamCreate01Queue {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))
    val context: SparkContext = streamingContext.sparkContext

    // 创建 RDD 队列
    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]

    // 创建 QueueInputDStream
    val inputDStream: InputDStream[Int] = streamingContext.queueStream(queue = queue, oneAtATime = false)
    val dStream: DStream[(Int, Int)] = inputDStream.map((i: Int) => (i, 1))
    val dStream2: DStream[(Int, Int)] = dStream.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    dStream2.print

    // 启动采集器
    streamingContext.start

    // 循环创建并向 RDD 队列中放入 RDD
    for (_ <- 1 to 5) {
      queue += context.makeRDD(seq = 1 to 300, numSlices = 10)
      Thread.sleep(2000)
    }

    // 等待采集器的关闭
    streamingContext.awaitTermination
  }

}
