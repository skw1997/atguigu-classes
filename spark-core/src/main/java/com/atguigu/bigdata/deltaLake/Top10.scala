package com.atguigu.bigdata.deltaLake

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable

case class Top10(date:String, hour:Int, topic:String, rank:Int, num:Int)

object Top10 {
  def main(args: Array[String]): Unit = {
    val input = "C:\\Users\\Public\\spark\\spark-3.1.1-bin-hadoop2.7\\bin\\data\\new-table"
    val output = "C:\\Users\\Public\\spark\\spark-3.1.1-bin-hadoop2.7\\bin\\data\\top10-table"

    val stSDF = new SimpleDateFormat("yyyy-MM-dd HH")

    val sparkConf= new SparkConf().setMaster("local[*]").setAppName("transForm")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val inputDF: DataFrame = spark.read.format("delta").load(input).toDF()

    inputDF.createOrReplaceTempView("t_basic")

    val queryDF: DataFrame = spark.sql("select ts, topic from t_basic")

    val rdd: RDD[(String, String)] = queryDF.map(
      line => {
        val ts = line.getLong(0)
        val hourStr: String = stSDF.format(new Date(ts))
        hourStr -> line.getString(1)
      }
    ).rdd

    val rddGrp = rdd.groupByKey()

    val result: RDD[(String, List[(String, Int)])] = rddGrp.mapValues(
      it => {
        val stringToInt: mutable.Map[String, Int] = mutable.Map[String, Int]()
        it.foreach(topic => {
          var count = stringToInt.getOrElse(topic, 0)
          count += 1
          stringToInt.put(topic, count)
        })
        val hourTopicList: List[(String, Int)] = stringToInt.toList
        val sortedList = hourTopicList.sortBy(_._2).reverse
        sortedList.take(10)
      }
    )


    val resultRdd: RDD[List[Top10]] = result.map(element => {
      val date = element._1.split(" ")(0)
      val hour = element._1.split(" ")(1)
      var counter = 0
      element._2.map(
        innerElement => {
          val topic = innerElement._1
          val num = innerElement._2
          counter += 1

          Top10(date = date, hour = hour.toInt, topic = topic, rank = counter, num = num)
        }
      )
    })

    val finalResult: RDD[Top10] = resultRdd.flatMap(x => x)

    finalResult.toDF().write.format("delta").save(output)

    val df = spark.read.format("delta").load(output)
    df.show()

  }

}
