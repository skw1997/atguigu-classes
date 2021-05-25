package com.atguigu.bigdata.deltaLake

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date


case class OutputDefine2(ts:Long,date:String, hour:Int, minute:Int, userID:String,
                        topic:String, resultRank:Int, clickRank:Int, url:String)
object transformV2 {
  def main(args: Array[String]): Unit = {
    val origFilePath = "C:\\Users\\Public\\spark\\xuexi\\code\\code\\atguigu-classes\\data"
    val outputPath = "C:\\Users\\Public\\spark\\spark-3.1.1-bin-hadoop2.7\\bin\\data\\new-table"

    val stSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateSDF = new SimpleDateFormat("yyyy-MM-dd")
    val hourSDF = new SimpleDateFormat("HH")
    val minSDF = new SimpleDateFormat("mm")
    val today = dateSDF.format(new Date)

    val sparkConf= new SparkConf().setMaster("local[*]").setAppName("transForm")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val lineRDD = sc.textFile(origFilePath).map(_.split("\t"))

    val resultRDD: RDD[OutputDefine2] = lineRDD.map(
      line => {
        val ts: Long = stSDF.parse(today + " " + line(0)).getTime
        val hour: Int = hourSDF.format(new Date(ts)).toInt
        val min: Int = minSDF.format(new Date(ts)).toInt

        val userID = line(1)
        val topic = line(2)
        val resultRank = line(3).toInt
        val clickRank = line(4).toInt
        val url = line(5)

        OutputDefine2(ts = ts, date = today, hour = hour, minute = min, userID = userID, topic = topic
          , resultRank = resultRank, clickRank = clickRank, url = url)
      }
    )

    resultRDD.toDF().write.format("delta").option("mergeSchema",true).mode("overwrite").save(outputPath)

    val df = spark.read.format("delta").load(outputPath)
    df.show()

    spark.close()

  }

}
