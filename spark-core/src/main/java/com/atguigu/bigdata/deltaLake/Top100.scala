package com.atguigu.bigdata.deltaLake
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable

case class Top100(date: String, topic: String, rank: Int, num: Int)
object Top100 {
  def main(args: Array[String]): Unit = {
    val input = "C:\\Users\\Public\\spark\\spark-3.1.1-bin-hadoop2.7\\bin\\data\\new-table"
    val output = "C:\\Users\\Public\\spark\\spark-3.1.1-bin-hadoop2.7\\bin\\data\\top100-table"

    val stSDF = new SimpleDateFormat("yyyy-MM-dd")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transForm")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val inputDF: DataFrame = spark.read.format("delta").load(input).toDF()

    inputDF.createOrReplaceTempView("t_basic")

    val queryDF: DataFrame = spark.sql("select ts, topic from t_basic")

    val rdd: RDD[(String, String)] = queryDF.map(
      line => {
        val ts = line.getLong(0)
        val dayStr: String = stSDF.format(new Date(ts))
        dayStr -> line.getString(1)
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
        sortedList.take(100)
      }
    )

    val resultRdd: RDD[List[Top100]] = result.map(element => {
      val date = element._1
      var counter = 0
      element._2.map(
        innerElement => {
          val topic = innerElement._1
          val num = innerElement._2
          counter += 1

          Top100(date = date, topic = topic, rank = counter, num = num)
        }
      )
    })

    val finalResult: RDD[Top100] = resultRdd.flatMap(x => x)

    finalResult.toDF().write.format("delta").save(output)

    spark.read.format("delta").load(output).repartition(4)
      .write.format("delta").mode("overwrite").save("C:\\Users\\Public\\spark\\spark-3.1.1-bin-hadoop2.7\\bin\\data\\top100-table-1")

    val df = spark.read.format("delta").load(output).toDF().show()


  }
}



