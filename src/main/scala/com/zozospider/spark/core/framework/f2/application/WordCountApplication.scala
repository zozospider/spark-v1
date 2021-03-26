package com.zozospider.spark.core.framework.f2.application

import com.zozospider.spark.core.framework.f2.common.ApplicationTrait
import com.zozospider.spark.core.framework.f2.controller.WordCountController

object WordCountApplication extends App with ApplicationTrait {

  run() {
    val controller: WordCountController = new WordCountController
    controller.execute()
  }

}
