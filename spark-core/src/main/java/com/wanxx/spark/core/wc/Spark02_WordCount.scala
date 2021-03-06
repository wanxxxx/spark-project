package com.wanxx.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//和wordcount01的区别，有reduce过程
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 建立和spark框架的连接
    //JDBC连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile(path = "data/1 ")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne= words.map {
      word => (word, 1)
    }

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)

    val wordCount = wordGroup.map {
      case (word, list) => {
        list.reduce {
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        }
      }
    }
    //5.打印
    wordCount.foreach(println)
    //TODO 关闭连接
    sc.stop()
  }

}
