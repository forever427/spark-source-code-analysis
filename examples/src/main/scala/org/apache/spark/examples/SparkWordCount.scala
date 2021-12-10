package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List("hello world hello spark", "hello world hello hadoop hello hello", "hello spark hello hadoop"))
    val mapedRDD = rdd.flatMap(_.split("\\s+")).map((_, 1))
    val reduceByKeyedRDD = mapedRDD.reduceByKey(_ + _)
    reduceByKeyedRDD.foreach(println)
  }
}
