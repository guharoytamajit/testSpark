package tutorial

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PairRDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val tupleList = sc.makeRDD(Seq((1, 2), (3, 6), (3, 4)))
    val other = sc.makeRDD(Seq((3, 9)))
    println("performing reduceByKey transformation:" + tupleList.reduceByKey((x, y) => x + y).collect().mkString("[", ",", "]"))
    println("performing groupByKey transformation:" + tupleList.groupByKey().collect().mkString("[", ",", "]"))
    println("performing mapValues transformation:" + tupleList.mapValues { x => x + 2 }.collect().mkString("[", ",", "]"))
    println("performing flatMapValues transformation:" + tupleList.flatMapValues { x => x to 5 }.collect().mkString("[", ",", "]"))
    println("performing keys transformation:" + tupleList.keys.collect().mkString("[", ",", "]"))
    println("performing values transformation:" + tupleList.values.collect().mkString("[", ",", "]"))

    println("performing subtractByKey transformation:" + tupleList.subtractByKey(other).collect().mkString("[", ",", "]"))
    println("performing join transformation:" + tupleList.join(other).collect().mkString("[", ",", "]"))
    println("performing rightOuterJoin transformation:" + tupleList.rightOuterJoin(other).collect().mkString("[", ",", "]"))
    println("performing leftOuterJoin transformation:" + tupleList.leftOuterJoin(other).collect().mkString("[", ",", "]"))
    println("performing leftOuterJoin transformation:" + tupleList.leftOuterJoin(other).collect().mkString("[", ",", "]"))
    println("performing cogroup transformation:" + tupleList.cogroup(other).collect().mkString("[", ",", "]"))

    println("performing countByKey action:" + tupleList.countByKey())
    println("performing collectAsMap action:" + tupleList.collectAsMap())
    println("performing lookup action:" + tupleList.lookup(3))

  }
}