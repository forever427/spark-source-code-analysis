package org.apache.spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("foreach").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val stus: RDD[Student] = sc.parallelize(List(
      Student(1, "胖胖", 3, 0.6),
      Student(1, "笨笨", 2, 0.5),
      Student(1, "大笨笨", 1, 0.5),
      Student(1, "灰灰", 25, 1.72),
      Student(1, "微微", 24, 1.65),
      Student(1, "小微微", 27, 1.65)))
    val sorted_stus = stus.sortBy(stu => stu, true, 1)
    sorted_stus.foreach(stu => println(stu))
    val top2 = sorted_stus.take(num = 2)
//    top2.foreach(stu => println(stu))
  }
}

case class Student(id: Int, name: String, age: Int, height: Double) extends Ordered[Student] {
  override def compare(that: Student): Int = {
    var value: Int = this.height.compare(that.height)
    if (value == 0) value = -(this.age.compareTo(that.age))
    println(s"${this.name} compare to ${that.name} value is ${value}")
    value
  }
}