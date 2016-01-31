package tutorial

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import breeze.stats.hist

object MlTest {
def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
val  user_data = sc.textFile("file:///d:/ml-100k/u.user")
println(user_data.first())
//hist(ages, bins=20, color='lightblue', normed=True)

}
}