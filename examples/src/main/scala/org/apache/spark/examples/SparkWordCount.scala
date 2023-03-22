package org.apache.spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {

    // 创建配置文件
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")

    val array = Array(1, 2, 3, 4, 5)

    // 创建sc
    val sc: SparkContext = new SparkContext(conf)

    // 创建rdd
    val rdd: RDD[String] = sc.parallelize(List("hello world hello spark", "hello world hello hadoop hello hello", "hello spark hello hadoop"))

    val mapedRDD: RDD[(String, Int)] = rdd.flatMap(_.split("\\s+")).map(x=>{val a = array.size;(x, 1)})

    val reduceByKeyedRDD: RDD[(String, Int)] = mapedRDD.reduceByKey(_ + _)

    reduceByKeyedRDD.foreach(println)
  }
}
