package com.zozospider.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming01WordCount {

  def main(args: Array[String]): Unit = {

    // 创建 SparkStreaming 运行环境
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    // batchDuration: 批量处理的周期设置为 3s
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    // 需要通过 netcat 往 localhost 的 9999 端口发送数据, 如下:
    // > nc -lp 9999
    // hello word
    // hello
    // hello

    // 逻辑处理
    // 获取端口数据
    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 9999)
    val dStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val dStream2: DStream[(String, Int)] = dStream.map((s: String) => (s, 1))
    val dStream3: DStream[(String, Int)] = dStream2.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    // 打印时间戳
    dStream3.print

    // 关闭环境 (不能这样做)
    // streamingContext.stop()
    // 注意: 由于 SparkStreaming 采集器是长期运行的任务, 所以不能直接关闭
    // 如果 main() 方法执行完毕, 应用程序也会自动结束, 所以不能让 main() 方法执行完毕
    // 1. 启动采集器
    streamingContext.start
    // 2. 等待采集器的关闭
    streamingContext.awaitTermination
  }

}
