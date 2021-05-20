package com.zozospider.spark.streaming.test

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

import java.text.SimpleDateFormat
import java.util.Date

class TestHandler extends Serializable {

  private val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val dateFormat2: SimpleDateFormat = new SimpleDateFormat("HH:mm")

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
    // 转换数据结构
    val dStream: DStream[(UserAd, Long)] = adLogDStream.map((adLog: AdLog) => {
      val dateStr: String = dateFormat.format(new Date(adLog.timestamp))
      (UserAd(dateStr, adLog.userId, adLog.adId), 1L)
    })
    val dStream2: DStream[(UserAd, Long)] = dStream.reduceByKey((l1: Long, l2: Long) => l1 + l2)
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

  // 统计每个广告的点击次数 (每个窗口)
  def toAdHMCount(adLogDStream: DStream[AdLog]): DStream[(AdHM, Long)] = {

    // 开窗: 时间间隔为 2 分钟的 window
    val dStream: DStream[AdLog] = adLogDStream.window(Minutes(2))

    // 转换数据结构: AdLog -> ((adId, dt), 1)
    val dStream2: DStream[(AdHM, Long)] = dStream.map((adLog: AdLog) => {
      val hmStr: String = dateFormat2.format(new Date(adLog.timestamp))
      (AdHM(adLog.adId, hmStr), 1L)
    })
    val dStream3: DStream[(AdHM, Long)] = dStream2.reduceByKey((l1: Long, l2: Long) => l1 + l2)
    dStream3
  }

  def groupAd(dStream: DStream[(AdHM, Long)]): DStream[(String, List[(String, Long)])] = {

    val dStream2: DStream[(String, (String, Long))] = dStream.map((tuple: (AdHM, Long)) =>
      (tuple._1.adId, (tuple._1.hm, tuple._2)))

    val dStream3: DStream[(String, Iterable[(String, Long)])] = dStream2.groupByKey()

    val dStream4: DStream[(String, List[(String, Long)])] = dStream3.mapValues((iterable: Iterable[(String, Long)]) => {
      iterable.toList.sortWith((tuple1: (String, Long), tuple2: (String, Long)) =>
        tuple1._1 < tuple2._1)
    })
    dStream4
  }

}
