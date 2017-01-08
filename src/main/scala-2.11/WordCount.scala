/**
  * Created by hadoop on 1/8/17.
  */

import org.apache.spark.sql.SparkSession


object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("WordCount").config("spark.some.config.option", "some-value").getOrCreate()

    val Data = spark.sparkContext.textFile("file:///home/hadoop/Desktop/Data.txt")

    val CData = Data.flatMap(x=> x.split(" ")).map(x=> (x,1)).reduceByKey(_ + _)

    CData.collect.foreach(println)

  }

}