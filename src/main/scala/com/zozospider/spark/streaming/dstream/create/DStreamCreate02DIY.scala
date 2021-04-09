package com.zozospider.spark.streaming.dstream.create

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Random

// DStream 创建 - 自定义数据源
object DStreamCreate02DIY {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    // 创建 ReceiverInputDStream
    val inputDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver)
    inputDStream.print

    streamingContext.start
    streamingContext.awaitTermination
  }

}

// 自定义数据采集器
class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  private var isStart: Boolean = false

  // 最初启动的时候, 调用该方法, 作用为: 读数据并将数据发送给 Spark
  override def onStart(): Unit = {
    isStart = true
    new Thread(() => {
      while (isStart) {
        val msg: String = "current msg is " + new Random().nextInt(10).toString
        store(msg)
        Thread.sleep(500)
      }
    }).start()
  }

  override def onStop(): Unit = {
    isStart = false
  }

}
