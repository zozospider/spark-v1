package com.zozospider.spark.streaming.test

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import java.text.SimpleDateFormat
import java.util.Date

class Test01AHandler extends Serializable {

  private val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  // 过滤在黑名单中的用户
  def filterByBlackList(adLogDStream: DStream[AdLog]): DStream[AdLog] = {
    val dStream: DStream[AdLog] = adLogDStream.filter((adLog: AdLog) => {
      val bool: Boolean = MyDbUtil.isExist(
        sql = "select * from black_list where userid = ?",
        params = Array(adLog.userId))
      !bool
    })
    dStream
  }

  // 统计每个用户点击次数 (每个采集周期)
  def toUserAdCount(adLogDStream: DStream[AdLog]): DStream[(UserAd, Long)] = {
    val dStream: DStream[(UserAd, Long)] = adLogDStream.map((adLog: AdLog) => {
      val dateStr: String = dateFormat.format(new Date(adLog.timestamp))
      (UserAd(dateStr, adLog.userId, adLog.adId), 1L)
    })
    val dStream2: DStream[(UserAd, Long)] = dStream.reduceByKey((i1: Long, i2: Long) => i1 + i2)
    dStream2
  }

  // 更新用户的广告当天点击次数
  def updateUserAdCount(dStream: DStream[(UserAd, Long)]): Unit = {
    dStream.foreachRDD((rdd: RDD[(UserAd, Long)]) => {
      rdd.foreachPartition((iterator: Iterator[(UserAd, Long)]) => {

        // TODO getConnection here

        iterator.foreach((tuple: (UserAd, Long)) => {
          val dt: String = tuple._1.dt
          val userId: String = tuple._1.userId
          val adId: String = tuple._1.adId
          val count: Long = tuple._2

          // 更新用户的广告当天点击次数
          MyDbUtil.executeUpdate(
            sql =
              """
                | INSERT INTO user_ad_count(dt, userid, adid, count)
                | VALUES (?, ?, ?, ?)
                | ON DUPLICATE KEY
                | UPDATE count = count + ?
              """.stripMargin,
            params = Array(dt, userId, adId, count, count))
        })
      })
    })
  }

  // D. 查询用户的当天点击次数
  //    如果点击次数超过点击阈值(20), 那么将用户拉入到黑名单
  def addToBlackListOrUpdateUserAdCount(dStream: DStream[(UserAd, Long)]): Unit = {
    dStream.foreachRDD((rdd: RDD[(UserAd, Long)]) => {
      rdd.foreachPartition((iterator: Iterator[(UserAd, Long)]) => {

        // TODO getConnection here

        iterator.foreach((tuple: (UserAd, Long)) => {
          val dt: String = tuple._1.dt
          val userId: String = tuple._1.userId
          val adId: String = tuple._1.adId

          // 查询用户的当天点击次数
          val count: Long = MyDbUtil.get[Long](
            sql = "select count from user_ad_count where dt = ? and userid = ? and adid = ?",
            params = Array(dt, userId, adId))

          // 如果点击次数超过点击阈值(20), 那么将用户拉入到黑名单
          if (count > 20) {
            MyDbUtil.executeUpdate(
              sql = "INSERT INTO black_list (userid) VALUES (?) ON DUPLICATE KEY UPDATE userid = ?",
              params = Array(userId, userId))
          }
        })
      })
    })
  }

}
