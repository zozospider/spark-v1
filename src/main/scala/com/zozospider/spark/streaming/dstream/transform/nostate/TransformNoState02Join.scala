package com.zozospider.spark.streaming.dstream.transform.nostate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// DStream 转换 - 无状态转化操作 - join()
object TransformNoState02Join {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(5))

    val inputDStreamA: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 8888)
    val inputDStreamB: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 9999)

    val dStreamA: DStream[(String, String)] = inputDStreamA.map((s: String) => (s, "A"))
    val dStreamB: DStream[(String, String)] = inputDStreamB.map((s: String) => (s, "B"))

    // DStream 的 join() 操作其实就是两个 rdd 的 join() 操作
    val dStream: DStream[(String, (String, String))] = dStreamA.join(dStreamB)
    dStream.print

    streamingContext.start
    streamingContext.awaitTermination
  }

}
