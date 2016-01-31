package tutorial

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Employee(name:String,id:Int,isManager:Boolean)

object MainSql {
def main(args: Array[String]): Unit = {
  val conf = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val emps=List(new Employee("jim",1,true),
  new Employee("jack",2,true),
  new Employee("ben",3,true),
  new Employee("alex",4,true))
  val df=emps.toDF()
  println(df.show())
  
  println(df.printSchema())
  val json = sqlContext.read.format("json").load("emp.json");
  
  println(json.show())
  println(json.printSchema())
  
  json.registerTempTable("Emp")
  val extracted = sqlContext.sql("select  age from Emp")
 extracted.show()
}
}