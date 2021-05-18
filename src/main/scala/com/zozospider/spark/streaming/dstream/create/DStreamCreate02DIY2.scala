package com.zozospider.spark.streaming.dstream.create

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

// DStream 创建 - 自定义数据源
object DStreamCreate02DIY2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    // 创建 ReceiverInputDStream
    val inputDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(
      new MyReceiver2(host = "localhost", port = 9999))
    inputDStream
      .flatMap((s: String) => s.split(" "))
      .map((s: String) => (s, 1))
      .reduceByKey((i1: Int, i2: Int) => i1 + i2)
      .print()

    streamingContext.start
    streamingContext.awaitTermination
  }

}

// 自定义数据采集器
class MyReceiver2(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  private var isStart: Boolean = _

  // 最初启动的时候, 调用该方法, 作用为: 读数据并将数据发送给 Spark
  override def onStart(): Unit = {

    isStart = true
    new Thread(() => {

      // 创建一个 Socket, BufferedReader 用于读取端口传来的数据
      val socket: Socket = new Socket(host, port)
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      var msg: String = reader.readLine

      while (isStart) {
        store(msg)
        msg = reader.readLine
      }

      reader.close()
      socket.close()

    }).start()
  }

  override def onStop(): Unit = {
    isStart = false
  }

}
