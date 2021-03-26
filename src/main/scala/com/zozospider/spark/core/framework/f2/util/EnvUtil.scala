package com.zozospider.spark.core.framework.f2.util

import org.apache.spark.SparkContext

object EnvUtil {

  private val threadLocalSparkContext: ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]

  def setSparkContext(context: SparkContext): Unit = {
    threadLocalSparkContext.set(context)
  }

  def getSparkContext: SparkContext = {
    threadLocalSparkContext.get()
  }

  def clearSparkContext(): Unit = {
    threadLocalSparkContext.remove()
  }

}
