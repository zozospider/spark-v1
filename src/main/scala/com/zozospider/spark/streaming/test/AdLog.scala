package com.zozospider.spark.streaming.test

import scala.collection.mutable.ListBuffer
import scala.util.Random

case class AdLog(timestamp: Long,
                 cityArea: String,
                 cityName: String,
                 userId: String,
                 adId: String)

case class UserAd(dt: String,
                  userId: String,
                  adId: String)

case class City(cityId: Long,
                cityArea: String,
                cityName: String)

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
