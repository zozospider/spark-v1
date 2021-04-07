package com.zozospider.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Sql01Basic2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQL").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val context: SparkContext = session.sparkContext
    import session.implicits._

    val rdd: RDD[(Int, String, Int)] = context.makeRDD(List((1, "one", 10), (2, "two", 20)))

    // RDD <=> DataFrame
    val dataFrame: DataFrame = rdd.toDF("id", "name", "age")
    dataFrame.show

    val rdd2: RDD[Row] = dataFrame.rdd
    rdd2.collect.foreach(println)

    println("******")

    // DataFrame <=> DataSet
    val dataSet: Dataset[User] = dataFrame.as[User]
    dataSet.show

    val dataFrame2: DataFrame = dataSet.toDF
    dataFrame2.show

    println("******")

    // RDD <=> DataSet
    /*val rdd3: RDD[User] = rdd.map {
      case (id, name, age) => User(id, name, age)
    }*/
    val rdd3: RDD[User] = rdd.map((tuple: (Int, String, Int)) => User(tuple._1, tuple._2, tuple._3))
    val dataSet2: Dataset[User] = rdd3.toDS
    dataSet2.show

    val rdd4: RDD[User] = dataSet2.rdd
    rdd4.collect.foreach(println)

    session.close
  }

}

case class User(id: Int, name: String, age: Int)
