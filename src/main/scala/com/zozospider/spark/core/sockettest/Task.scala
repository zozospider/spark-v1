package com.zozospider.spark.core.sockettest

class Task extends Serializable {

  val data: List[Int] = List(1, 2, 3, 4)
  // val logic: Int => Int = (i: Int) => i * 2
  val logic: Int => Int = _ * 2

  // 计算
  def compute(): Seq[Int] = {
    data.map(logic)
  }

}
