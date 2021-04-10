package com.zozospider.spark.streaming.close

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

// 优雅关闭
object StreamingClose01Close {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    // 流式任务需要 7*24 小时执行, 但是有时涉及到升级代码需要主动停止程序
    // 但是分布式程序, 没办法做到一个个进程去杀死, 所有配置优雅的关闭就显得至关重要了
    // 使用外部文件系统来控制内部程序关闭

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 9999)
    val dStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val dStream2: DStream[(String, Int)] = dStream.map((s: String) => (s, 1))
    val dStream3: DStream[(String, Int)] = dStream2.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    dStream3.print

    // 启动关闭监听线程
    // 优雅关闭: 计算节点不再接收新的数据, 而是将现有的数据处理完毕, 然后关闭
    new Thread(new MonitorStop(streamingContext)).start()

    streamingContext.start
    streamingContext.awaitTermination
  }

}

// 关闭监听类
class MonitorStop(streamingContext: StreamingContext) extends Runnable {

  override def run(): Unit = {
    // 循环判断是否需要退出
    while (true) {
      Thread.sleep(10000)
      // 从第三方 (如 MySQL, Redis, HDFS, File 等) 获取是否退出标识
      // 此处模拟退出测试
      val stopFlag: Boolean = true
      if (stopFlag) {
        // 优雅关闭: 计算节点不再接收新的数据, 而是将现有的数据处理完毕, 然后关闭
        if (streamingContext.getState == StreamingContextState.ACTIVE) {
          streamingContext.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }

  }

}
