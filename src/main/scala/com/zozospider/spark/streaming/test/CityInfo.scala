package com.zozospider.spark.streaming.test

import scala.collection.mutable.ListBuffer
import scala.util.Random

case class City(id: Long, name: String, area: String)

case class RandomValue[T](value: T, weight: Int)

class RandomGetter[T] {
  var totalWeight: Int = 0
  var valueBuffer: ListBuffer[T] = new ListBuffer[T]

  def randomValue: T = {
    val i: Int = new Random().nextInt(totalWeight)
    valueBuffer(i)
  }

}

object RandomGetter {
  def apply[T](values: RandomValue[T]*): RandomGetter[T] = {
    val getter: RandomGetter[T] = new RandomGetter[T]()
    for (value <- values) {
      getter.totalWeight += value.weight
      for (_ <- 1 to value.weight) {
        getter.valueBuffer += value.value
      }
    }
    getter
  }
}
