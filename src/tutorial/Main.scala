package tutorial

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Main {
def main(args: Array[String]): Unit = {
  val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
  val sc = new SparkContext(conf)
  
  val test = sc.textFile("food.txt")
  val words = test.flatMap { line => line.split(" ") }
  val lowerCaseWords = words.map { word => word.toLowerCase() }
  val filteredWords = lowerCaseWords.filter { word => word.matches("[a-zA-z]+") }
 
  val tokens = filteredWords.map { word => (word,1) }
  val tuples = tokens.reduceByKey((a,v)=>a+v)
  val sortedTuples = tuples.sortBy(tup=>tup._2,false) 
  sortedTuples.saveAsTextFile("weight")
}
}