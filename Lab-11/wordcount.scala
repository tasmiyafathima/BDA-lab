package scalawordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.immutable.ListMap

object wordcount {
  def main (args: Array[String]) {
  val conf = new SparkConf().setAppName("WordCount").setMaster("local")
  val sc = new SparkContext(conf)
  val textFile = sc.textFile("input.txt")
  val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
  val sorted=ListMap(counts.collect.sortWith(_._2 > _._2):_*)// sort in descending order based on values
  println(sorted)
  for((k,v)<-sorted)
  {
    if(v>4)
    {
       print(k+",")
       print(v)
       println()
     }
  }
  }
}