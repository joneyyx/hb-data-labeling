package com.cummins.dataLabeling.util

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col


object Util extends Serializable {
  val spark = getSpark()

  def getSpark(): SparkSession = {
    try {
      SparkSession.builder()
//        .master("local")
        .appName("hbdata_labeling")
        .enableHiveSupport()
        .config("spark.warehouse.dir",new File("spark-warehouse").getPath)
        .config("spark.shuffle.service.enabled", true)
        .config("spark.driver.maxResultSize", "4G")
        .config("spark.sql.parquet.writeLegacyFormat", true)
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()
    } catch {
      case e: Exception => {
        SparkSession.builder().
          master("local[2]").
          appName("Spark Locally").
          getOrCreate()
      }
    }
  }

  spark.conf.set("spark.sql.shuffle.partitions", 400)


  def getWarehousePath(warehouseRootPath: String, database: String = "default", tableName: String): String = {
    var dbname = database.toLowerCase
    var layer = "na"
    if (dbname.contains("_raw") || dbname.contains("raw_tel")) {
      dbname = dbname.replaceAll("_raw", "")
      layer = "raw"
    }
    else if (dbname.contains("_primary")) {
      dbname = dbname.replaceAll("_primary", "")
      layer = "primary"
    }
    else if (dbname.contains("_features") || dbname.contains("_feature")) {
      dbname = dbname.replaceAll("_features", "")
      dbname = dbname.replaceAll("_feature", "")
      layer = "features"
    }
    else if (dbname.contains("_mi") || dbname.contains("_modelinput")) {
      dbname = dbname.replaceAll("_modelinput", "")
      dbname = dbname.replaceAll("_mi", "")
      layer = "mi"
    }
    else if (dbname.contains("_modeloutput")) {
      dbname = dbname.replaceAll("_modeloutput", "")
      dbname = dbname.replaceAll("_mo", "")
      layer = "mo"
    }
    else if (dbname.contains("_rpt") || dbname.contains("_report")) {
      dbname = dbname.replaceAll("_report", "")
      dbname = dbname.replaceAll("_rpt", "")
      layer = "rpt"
    }
    else if (dbname.contains("_reference") || dbname.contains("_monitoring")) {
      dbname = dbname.replaceAll("_reference", "")
      layer = "reference"
    }

    if (dbname == "pri_tel" || dbname == "raw_tel") {
      warehouseRootPath.toLowerCase + "/" + dbname + "/" + tableName.toLowerCase

    }
    else
      warehouseRootPath.toLowerCase + "/" + layer + "/" + dbname + "/" + tableName.toLowerCase
    //		for pipeline monitoring job, rewrite the hdfs path.
    //		warehouseRootPath.toLowerCase + "/" + layer + "/"  + tableName.toLowerCase
  }

  //Function to Capitalize and format(remove spaces/spl characters) from column names
  def formatCapitalizeNames(dataFrame: DataFrame): DataFrame = {
    val capitalizedNames = dataFrame.columns.map(colname => colname.replaceAll("[(|)]", "").replaceAll(" ", "_").replaceAll("/", "").replaceAll("\\\\", "").replaceAll("-", "_").toUpperCase)
    val originalNames = dataFrame.columns
    dataFrame.select(List.tabulate(originalNames.length)(i => col(originalNames(i)).as(capitalizedNames(i))): _*)
  }

  def saveDataWithMultipePartitions(df: DataFrame, db: String, tbl: String, path: String, ptkey: List[String] ,mode: String = "overwrite", format: String = "parquet"): Unit = {
    formatCapitalizeNames(df).write.
      options(Map("path" -> getWarehousePath(path, db, tbl))).
      mode(mode).format(format).partitionBy(ptkey(0), ptkey(1)).
      saveAsTable(s"$db.$tbl")
  }



}
