package com.zozospider.spark.streaming.dstream.transform.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// DStream 转换 - 有状态转化操作 - window()
object TransformState02Window {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    // batchDuration: 批量处理的周期设置为 3s
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))
    // 采用 E 方式时需要指定 checkpoint 目录
    streamingContext.checkpoint("checkpoint")

    // Window Operations 可以设置窗口的大小和滑动窗口的间隔来动态的获取当前 Steaming 的允许状态

    // 所有基于窗口的操作都需要两个参数, 分别为窗口时长和滑动步长:
    // windowDuration (窗口时长): 计算内容的时间范围 (必须是 batchDuration 的整数倍)
    // slideDuration (滑动步长): 隔多久触发一次计算 (必须是 batchDuration 的整数倍)

    // 是否有重复统计:
    // slideDuration (滑动步长) < windowDuration (窗口时长): 有重复统计
    // slideDuration (滑动步长) >= windowDuration (窗口时长): 没有重复统计

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 9999)
    val dStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val dStream2: DStream[(String, Int)] = dStream.map((s: String) => (s, 1))

    // A. 窗口 12 秒, 滑步 3 秒 (默认为 batchDuration) (有重复统计)
    /*val dStreamA: DStream[(String, Int)] = dStream2.window(windowDuration = Seconds(12))
    val dStreamA2: DStream[(String, Int)] = dStreamA.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    dStreamA2.print*/

    // B. 窗口 6 秒, 滑步 6 秒 (没有重复统计)
    // 窗口可以滑动, 但是默认情况下, 一个采集进行滑动, 这样的话可能会出现重复数据的计算, 为了避免这种情况, 可以改变滑动的步长 (如窗口和滑步都为 6s)
    /*val dStreamB: DStream[(String, Int)] = dStream2.window(windowDuration = Seconds(6), slideDuration = Seconds(6))
    val dStreamB2: DStream[(String, Int)] = dStreamB.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    dStreamB2.print*/

    // C. 窗口 12 秒, 滑步 6 秒 (有重复统计)
    /*val dStreamC: DStream[(String, Int)] = dStream2.window(windowDuration = Seconds(12), slideDuration = Seconds(6))
    val dStreamC2: DStream[(String, Int)] = dStreamC.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    dStreamC2.print*/

    // D. 窗口 12 秒, 滑步 6 秒 (有重复统计)
    /*val dStreamD: DStream[(String, Int)] = dStream2.reduceByKeyAndWindow(reduceFunc = {
      (i1: Int, i2: Int) => i1 + i2
    }, windowDuration = Seconds(12), slideDuration = Seconds(6))
    dStreamD.print*/

    // E. 窗口 12 秒, 滑步 6 秒 (有重复统计)
    // 当窗口较大, 滑步较小时, 可以采用增加数据和删除数据的方式, 无需重复计算 (此处不是重复统计), 提升性能
    // 注: 此方式需要指定 checkpoint 目录
    val dStreamE: DStream[(String, Int)] = dStream2.reduceByKeyAndWindow(reduceFunc = {
      (i1: Int, i2: Int) => i1 + i2
    }, invReduceFunc = {
      (i1: Int, i2: Int) => i1 - i2
    }, windowDuration = Seconds(12), slideDuration = Seconds(6))
    dStreamE.print

    streamingContext.start
    streamingContext.awaitTermination
  }

}
