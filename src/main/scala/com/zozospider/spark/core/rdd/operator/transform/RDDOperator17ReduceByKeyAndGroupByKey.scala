package com.zozospider.spark.core.rdd.operator.transform

// 转换算子 - Key - Value 类型 - reduceByKey() groupByKey()
object RDDOperator17ReduceByKeyAndGroupByKey {

  def main(args: Array[String]): Unit = {

    // reduceByKey 和 groupByKey 的区别:

    // 从 shuffle 的角度:
    // reduceByKey 和 groupByKey 都存在 shuffle 的操作,
    // 但是 reduceByKey 可以在 shuffle 前对分区内相同 key 的数据进行预聚合 (combine) 功能, 这样会减少落盘的数据量,
    // 而 groupByKey 只是进行分组, 不存在数据量减少的问题, reduceByKey 性能比较高

    // 从功能的角度:
    // reduceByKey 其实包含分组和聚合的功能,
    // groupByKey 只能分组不能聚合,
    // 所以在分组聚合的场合下, 推荐使用 reduceByKey, 如果仅仅是分组而不需要聚合, 那么只能使用 groupByKey

    // reduceByKey 分区内和分区间计算规则是相同的
  }

}
