package com.zozospider.spark.core.framework.f2.common

import com.zozospider.spark.core.framework.f2.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait ApplicationTrait {

  def run(name: String = "App", master: String = "local[*]")(f: => Unit): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(name).setMaster(master)
    val context: SparkContext = new SparkContext(conf)
    EnvUtil.setSparkContext(context)

    try {
      f
    } catch {
      case ex: Exception => println(s"Exception: $ex")
    }

    context.stop
    EnvUtil.clearSparkContext()
  }

}
