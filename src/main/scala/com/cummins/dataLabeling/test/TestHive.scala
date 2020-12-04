package com.cummins.dataLabeling.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
object TestHive {
  def main(args: Array[String]): Unit = {
    val spark =  SparkSession.builder()
      //        .master("local")
      .appName("Spark Locally")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .config("spark.sql.sources.bucketing.enabled",true)
      .config("hive.enforce.bucketing",true)
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("hive.exec.dynamic.partition",true)
      .enableHiveSupport()
      .getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    spark.conf.set("hive.exec.dynamic.partition",true)
    import spark._
    spark.read.orc(s"wasbs://westlake@dldevblob4qastorage.blob.core.chinacloudapi.cn/westlake_data/raw/telematics_hb_data/HB_PROCESSED_DATA_DAY/DF/20200101/*")
      .createOrReplaceTempView("hb_rawdata")

    spark.sql("select h64 as esn,h58 as altitude from hb_rawdata")
      .withColumn("report_date",lit("20200101"))
      .write
//      .format("Hive")
      .mode("append")
      .partitionBy("report_date")
      .bucketBy(3,"esn")

//        .save("/test/ybz/dfdata/")
//      .saveAsTable("test.testbucket")

    spark.close()
  }
}
