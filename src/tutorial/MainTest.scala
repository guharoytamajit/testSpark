package tutorial

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MainTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val list = sc.parallelize(List(1, 2, 3, 4, 5))
    val fruitList = sc.parallelize(List("orange", "apple", "grapes", "mango", "apple"))
    val lines = sc.parallelize(List("How are you", "I am fine"))

    println("performing map transformation: " + list.map(n => n * 2).collect().mkString("[", ",", "]"))
    println("performing flatmap transformation: " + lines.flatMap { x => x.split(" ") }.collect().mkString("[", ",", "]"))
    println("performing filter transformation: " + list.filter { x => x % 2 == 0 }.collect().mkString("[", ",", "]"))

    println("performing distinct transformation: " + sc.parallelize(List(1, 2, 1, 5)).distinct().collect().mkString("[", ",", "]"))
    println("performing union transformation: " + sc.parallelize(List(1, 2, 8, 15)).union(list).collect().mkString("[", ",", "]"))
    println("performing subtract transformation: " + sc.parallelize(List(1, 2, 8, 15)).subtract(list).collect().mkString("[", ",", "]"))
    println("performing intersect transformation: " + sc.parallelize(List(1, 2, 8, 15)).intersection(list).collect().mkString("[", ",", "]"))
    println("performing cartesian transformation: " + sc.parallelize(List(1, 2, 8, 15)).cartesian(fruitList).collect().mkString("[", ",", "]"))

    //actions
    println("performing collect action: " + list.collect().mkString("[", ",", "]"))
    println("performing count action: " + list.count())
    println("performing countByValue action: " + fruitList.countByValue())
    println("performing take action(order may change): " + list.take(3).mkString("[", ",", "]"))
    println("performing top(sorted) action: " + list.top(3).mkString("[", ",", "]"))
    println("performing takeOrdered(sorted) action: " + list.takeOrdered(3)(Ordering[Int].on { x => -x }).mkString("[", ",", "]"))
    println("performing takeSample action(withreplacement=false ie. no repeat): " + list.takeSample(false, 4).mkString("[", ",", "]"))
    println("performing takeSample action(withreplacement=true ie. allow repeat): " + list.takeSample(true, 4).mkString("[", ",", "]"))
    println("performing reduce action): " + list.reduce((x, y) => x * y))
    println("performing fold action): " + list.fold(1)((x, y) => x * y)) //for sum start value should be 0
    println("performing foreach action): ")
    list.foreach { print(_) }

  }
}