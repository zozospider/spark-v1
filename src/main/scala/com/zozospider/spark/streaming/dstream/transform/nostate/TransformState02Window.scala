package com.zozospider.spark.streaming.dstream.transform.nostate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// DStream 转换 - 有状态转化操作 - window()
object TransformState02Window {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    // Window Operations 可以设置窗口的大小和滑动窗口的间隔来动态的获取当前 Steaming 的允许状态
    // 所有基于窗口的操作都需要两个参数, 分别为窗口时长以及滑动步长
    // 窗口时长: 计算内容的时间范围
    // 滑动步长: 隔多久触发一次计算
    // 注意: 这两者都必须为采集周期大小的整数倍

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 9999)
    val dStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val dStream2: DStream[(String, Int)] = dStream.map((s: String) => (s, 1))

    // A. 窗口的范围应该是采集周期的整数倍
    // 3 秒一个批次, 窗口 12 秒 (存在重复统计)
    /*val dStreamA: DStream[(String, Int)] = dStream2.window(windowDuration = Seconds(12))
    val dStreamA2: DStream[(String, Int)] = dStreamA.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    dStreamA2.print*/

    // B. 窗口可以滑动, 但是默认情况下, 一个采集进行滑动, 这样的话可能会出现重复数据的计算, 为了避免这种情况, 可以改变滑动的步长 (如窗口和滑步都为 6s)
    // 3 秒一个批次, 窗口 12 秒, 滑步 6 秒 (存在重复统计)
    /*val dStreamB: DStream[(String, Int)] = dStream2.window(windowDuration = Seconds(12), slideDuration = Seconds(6))
    val dStreamB2: DStream[(String, Int)] = dStreamB.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    dStreamB2.print*/

    // C. 与 B 效果一样
    // 3 秒一个批次, 窗口 12 秒, 滑步 6 秒 (存在重复统计)
    val dStreamC: DStream[(String, Int)] = dStream2.reduceByKeyAndWindow(reduceFunc = {
      (i1: Int, i2: Int) => i1 + i2
    }, windowDuration = Seconds(12), slideDuration = Seconds(6))
    dStreamC.print

    streamingContext.start
    streamingContext.awaitTermination
  }

}
