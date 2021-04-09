package com.zozospider.spark.streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 有状态转化操作
// 相关内容见 DStreamTransform01State.scala
object StreamingWordCount2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))
    streamingContext.checkpoint("checkpoint")

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 9999)
    val dStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val dStream2: DStream[(String, Int)] = dStream.map((s: String) => (s, 1))
    val dStream3: DStream[(String, Int)] = dStream2.updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
      val currentCount: Int = seq.sum
      val previousCount: Int = option.getOrElse(0)
      Option(currentCount + previousCount)
    })
    dStream3.print

    streamingContext.start
    streamingContext.awaitTermination
  }

}
