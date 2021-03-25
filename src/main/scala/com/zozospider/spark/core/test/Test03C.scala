package com.zozospider.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 需求 C: 页面单跳转换率统计
// 需求分析图解见 [1.笔记 - 06-课程内容讲解.bmpr - RDD - 案例实操]
object Test03C {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    // 读取原始日志, 缓存提高性能
    val rdd: RDD[String] = context.textFile("data-dir\\test\\user_visit_action_simple.txt")
    rdd.cache

    // 将每一行原始数据转换为一个 UserVisitAction 样例类
    val actionRDD: RDD[UserVisitAction] = rdd.map((s: String) => {
      val fields: Array[String] = s.split("_")
      UserVisitAction(date = fields(0),
        user_id = fields(1).toLong,
        session_id = fields(2),
        page_id = fields(3).toLong,
        action_time = fields(4),
        search_keyword = fields(5),
        click_category_id = fields(6).toLong,
        click_product_id = fields(7).toLong,
        order_category_ids = fields(8),
        order_product_ids = fields(9),
        pay_category_ids = fields(10),
        pay_product_ids = fields(11),
        city_id = fields(12).toLong)
    })

    // 1. 计算分子
    val numeratorArray: Array[((Long, Long), Int)] = numerator(actionRDD)
    numeratorArray.take(10).foreach(println)
    println("------")

    // 2. 计算分母
    val denominatorArray: Array[(Long, Int)] = denominator(actionRDD)
    denominatorArray.take(10).foreach(println)
    println("------")

    // 3. 计算单跳转换率: 分子 / 分母
    numeratorArray.foreach((tuple: ((Long, Long), Int)) => {
      val i: Int = denominatorArray.toMap.getOrElse(tuple._1._1, 0)
      if (i != 0) {
        val result: Double = tuple._2.toDouble / i
        println(s"页面 ${tuple._1._1} 跳转到页面 ${tuple._1._2} 的单跳转换率为: $result")
      } else {
        println("ERROR, it should not be happen, please check the program.")
      }
    })

    context.stop
  }

  // 计算分子
  def numerator(actionRDD: RDD[UserVisitAction]): Array[((Long, Long), Int)] = {

    // 根据 session ID 分组: key (session ID) - value (多个 action)
    val rdd: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy((action: UserVisitAction) => action.session_id)

    // 多次转换 (只对 value 做转换)
    val rdd2: RDD[(String, List[((Long, Long), Int)])] = rdd.mapValues((iterable: Iterable[UserVisitAction]) => {
      // 根据访问时间排序 (升序): (action, action, action, action)
      val list: List[UserVisitAction] = iterable.toList.sortBy((action: UserVisitAction) => action.action_time)
      // 取出页面 ID 字段: (10, 20, 30, 40)
      val list2: List[Long] = list.map((action: UserVisitAction) => action.page_id)
      // 将相邻的两个页面 ID 连在一起: (10, 20, 30, 40) -> ((10, 20), (20, 30), (30, 40))
      // 方式一: Sliding() 滑窗
      // 方式二: zip() 拉链: ((10, 20, 30, 40) 和 (20, 30, 40))
      val list3: List[(Long, Long)] = list2.zip(list2.tail)
      // 转换成: (((10, 20), 1), ((20, 30), 1), ((30, 40), 1))
      val list4: List[((Long, Long), Int)] = list3.map((tuple: (Long, Long)) => (tuple, 1))
      list4
    })

    // 多次转换
    val rdd3: RDD[((Long, Long), Int)] = rdd2
      // 去掉 key, 只要 value (((10, 20), 1), ((20, 30), 1), ((30, 40), 1))
      .map((tuple: (String, List[((Long, Long), Int)])) => tuple._2)
      // 拆开 ((10, 20), 1), ((20, 30), 1), ((30, 40), 1)
      .flatMap((list: List[((Long, Long), Int)]) => list)

    // 统计多个跳转组合页面的次数
    val rdd4: RDD[((Long, Long), Int)] = rdd3.reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rdd4.collect
  }

  // 计算分母
  def denominator(actionRDD: RDD[UserVisitAction]): Array[(Long, Int)] = {

    val rdd: RDD[(Long, Int)] = actionRDD.map((action: UserVisitAction) => (action.page_id, 1))
      .reduceByKey((i1: Int, i2: Int) => i1 + i2)
    rdd.collect
  }

}

// 用户访问动作样例类
case class UserVisitAction(date: String, // 用户点击行为的日期
                           user_id: Long, // 用户 ID
                           session_id: String, // Session ID
                           page_id: Long, // 某个页面的 ID
                           action_time: String, // 动作的时间点
                           search_keyword: String, // 用户搜索的关键词
                           click_category_id: Long, // 某一个商品品类的 ID
                           click_product_id: Long, // 某一个商品的 ID
                           order_category_ids: String, // 一次订单中所有品类的 ID 集合
                           order_product_ids: String, // 一次订单中所有商品的 ID 集合
                           pay_category_ids: String, // 一次支付中所有品类的 ID 集合
                           pay_product_ids: String, // 一次支付中所有商品的 ID 集合
                           city_id: Long // 城市 id
                          )
