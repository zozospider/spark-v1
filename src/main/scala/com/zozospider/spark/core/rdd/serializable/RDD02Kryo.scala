package com.zozospider.spark.core.rdd.serializable

// 序列化 - Kryo 序列化框架
object RDD02Kryo {

  def main(args: Array[String]): Unit = {

    // 参考地址: https://github.com/EsotericSoftware/kryo

    // Java 的序列化能够序列化任何的类. 但是比较重 (字节多), 序列化后, 对象的提交也比较大
    // Spark 出于性能的考虑, Spark 2.0 开始支持另外一种 Kryo 序列化机制, Kryo 速度是 Serializable 的 10 倍
    // 当 RDD 在 Shuffle 数据的时候, 简单数据类型, 数组和字符串类型已经在 Spark 内部使用 Kryo 来序列化

  }

}
