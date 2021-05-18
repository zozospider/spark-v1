package com.zozospider.spark.streaming.dstream.transform.state

// DStream 转换 - 有状态转化操作 - 其他 window() 方法
object TransformState02Window2 {

  def main(args: Array[String]): Unit = {

    // window(windowLength, slideInterval): 基于对源 DStream 窗化的批次进行计算返回一个新的 DStream

    // countByWindow(windowLength, slideInterval): 返回一个滑动窗口计数流中的元素个数

    // reduceByWindow(func, windowLength, slideInterval): 通过使用自定义函数整合滑动区间流元素来创建一个新的单元素流

    // reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks]):
    //     当在一个 (K, V) 对的 DStream 上调用此函数, 会返回一个新(K, V)对的 DStream
    //     此处通过对滑动窗口中批次数据使用 reduce 函数来整合每个 key 的 value 值

    // reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks]):
    //     这个函数是上述函数的变化版本, 每个窗口的 reduce 值都是通过用前一个窗的 reduce 值来递增计算
    //     通过 reduce 进入到滑动窗口数据并 "反向 reduce" 离开窗口的旧数据来实现这个操作
    //     一个例子是随着窗口滑动对 keys 的 "加" "减" 计数
    //     通过前边介绍可以想到, 这个函数只适用于 "可逆的 reduce 函数", 也就是这些 reduce 函数有相应的 "反 reduce" 函数 (以参数 invFunc 形式传入)
    //     如前述函数, reduce 任务的数量通过可选参数来配置

    // countByWindow() 和 countByValueAndWindow() 作为对数据进行计数操作的简写
    // countByWindow() 返回一个表示每个窗口中元素个数的 DStream
    // countByValueAndWindow() 返回的 DStream 则包含窗口中每个值的个数
  }

}
