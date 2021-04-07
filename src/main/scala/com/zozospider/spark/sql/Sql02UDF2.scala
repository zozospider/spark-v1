package com.zozospider.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Sql02UDF2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQL").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // UDF: 自定义求年龄的平均值

    val dataFrame: DataFrame = session.read.json("data-dir\\sql\\user.json")
    dataFrame.createOrReplaceTempView("user")

    session.udf.register(name = "myAvgUDF", new MyAvgUDF)
    session.sql("select myAvgUDF(age) from user").show

    session.close
  }

}

// 自定义 UDF
class MyAvgUDF extends UserDefinedAggregateFunction {

  // 聚合函数输入参数的数据类型
  override def inputSchema: StructType = StructType(Array(
    StructField("age", LongType)
  ))

  // 聚合函数缓冲区中值的数据类型 (ageSum, count)
  override def bufferSchema: StructType = StructType(Array(
    StructField("ageSum", LongType),
    StructField("count", LongType)
  ))

  // 函数返回值的数据类型
  override def dataType: DataType = LongType

  // 稳定性: 对于相同的输入是否一直返回相同的输出
  override def deterministic: Boolean = true

  // 函数缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 索引 0 存年龄的总和 ageSum
    // 索引 1 存年龄的个数 count
    // buffer.update(0, 0L)
    // buffer.update(1, 0L)
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 更新缓冲区中的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 合并缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = {
    // ageSum / count
    buffer.getLong(0) / buffer.getLong(1)
  }

}
