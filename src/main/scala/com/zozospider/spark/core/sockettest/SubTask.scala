package com.zozospider.spark.core.sockettest

class SubTask extends Serializable {

  var data: List[Int] = _
  // val logic: Int => Int = (i: Int) => i * 2
  var logic: Int => Int = _

  // 计算
  def compute(): Seq[Int] = {
    data.map(logic)
  }

}
