package com.zozospider.spark.streaming.dstream.transform.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// DStream 转换 - 有状态转化操作 - updateStateByKey()
// 相关内容见 StreamingWordCount2.scala
object TransformState01UpdateStateByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    // UpdateStateByKey 原语用于记录历史记录, 有时, 我们需要在 DStream 中跨批次维护状态 (例如流计算中累加 WordCount)
    // 针对这种情况, updateStateByKey() 为我们提供了对一个状态变量的访问, 用于键值对形式的 DStream
    // 给定一个由 (键, 事件) 对构成的 DStream, 并传递一个指定如何根据新的事件更新每个键对应状态的函数, 它可以构建出一个新的 DStream, 其内部数据为 (键, 状态) 对
    // updateStateByKey() 的结果会是一个新的 DStream, 其内部的 RDD 序列是由每个时间区间对应的 (键, 状态) 对组成的
    // updateStateByKey() 操作使得我们可以在用新信息进行更新时保持任意的状态. 为使用这个功能, 需要做下面两步:
    // 1. 定义状态, 状态可以是一个任意的数据类型
    // 2. 定义状态更新函数, 用此函数阐明如何使用之前的状态和来自输入流的新值对状态进行更新
    // 使用 updateStateByKey() 需要对检查点目录进行配置, 会使用检查点来保存状态

    // 需要指定 checkpoint 路径用于保存缓冲区的状态数据
    streamingContext.checkpoint("checkpoint")

    // 有状态转化操作的 WordCount 如下:

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 9999)
    val dStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val dStream2: DStream[(String, Int)] = dStream.map((s: String) => (s, 1))

    // 无状态数据操作, 只对当前的采集周期内的数据进行处理
    // 在某些场合下, 需要保留数据统计结果 (状态), 实现数据的汇总
    // val dStream3: DStream[(String, Int)] = dStream2.reduceByKey((i1: Int, i2: Int) => i1 + i2)

    // updateStateByKey 根据 key 对数据的状态进行更新
    // seq 表示相同 key 的 value
    // option 表示缓冲区相同 key 的 value
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
