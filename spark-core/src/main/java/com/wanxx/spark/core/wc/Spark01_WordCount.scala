package com.wanxx.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 建立和spark框架的连接
    //JDBC连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作
    //1.按行读取文件
    val lines: RDD[String] = sc.textFile(path = "data/1 ")
    //2.将每行数据拆分为单个单词
    //将整体拆分为个体————扁平化
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //3.根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    //4.将分组的数据进行转换
    val wordCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    //5.打印
    val tuples: Array[(String, Int)] = wordCount.collect()
    tuples.foreach(println)
    //TODO 关闭连接
    sc.stop()
  }

}
