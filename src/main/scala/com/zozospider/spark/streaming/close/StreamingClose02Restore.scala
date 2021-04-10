package com.zozospider.spark.streaming.close

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 恢复数据
object StreamingClose02Restore {

  def main(args: Array[String]): Unit = {

    // TODO 未测试, 不知道啥意思, 额...

    val context: StreamingContext = StreamingContext.getActiveOrCreate(checkpointPath = "checkpoint", creatingFunc = () => {

      val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
      val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))
      streamingContext.checkpoint("checkpoint")

      val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 9999)
      val dStream: DStream[String] = inputDStream.flatMap(_.split(" "))
      val dStream2: DStream[(String, Int)] = dStream.map((s: String) => (s, 1))
      val dStream3: DStream[(String, Int)] = dStream2.reduceByKey((i1: Int, i2: Int) => i1 + i2)
      dStream3.print

      streamingContext
    })

    context.start
    context.awaitTermination
  }

}
