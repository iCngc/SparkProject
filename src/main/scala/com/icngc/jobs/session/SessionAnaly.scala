package com.icngc.jobs.session

import org.apache.spark.sql.SparkSession

/**
  * @author: Mr.Wang
  * @create: 2018-11-26 22:28
  */
object SessionAnaly {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().master("local[*]").appName("SessionAnaly").getOrCreate()
  }
}
