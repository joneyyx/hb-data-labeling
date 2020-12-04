package com.cummins.dataLabeling.primary.telematics

import java.sql.{Date, DriverManager}
import java.text.SimpleDateFormat
import java.util.Properties

import com.cummins.dataLabeling.common.config.AppConfig
import com.cummins.dataLabeling.common.constants.Constants
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

import scala.util.matching.Regex

object EsnMasterJob extends Serializable {

  val logger = Logger.getLogger(this.getClass)

  def processMasterData(spark: SparkSession,sc: SparkContext,filePath:String): Unit ={
    import spark.sql
    val prop = new Properties()
    prop.put("driver", AppConfig.sqlDriver)
    prop.put("user", AppConfig.sqlUser)
    prop.put("password", AppConfig.sqlPW)


    val engineDetailDF = spark.sql(
      s"""
				 |SELECT
				 |ESN_NEW AS ESN,
				 |ENGINE_PLATFORM,
				 |EMISSION_LEVEL,
				 |ENGINE_PLANT,
				 |DISPLACEMENT,
				 |BUILD_DATE,
				 |DOM_INT
				 | FROM ${Constants.Primary_Engine_Build}
		""".stripMargin).where("(Dom_Int!='Int'or Dom_Int is null) and Build_Date>=to_date('2014-01-01') and Build_Date<=CURRENT_DATE() and Emission_Level in ('E5','NS5','NS6','S5')")
      .withColumnRenamed("engine_serial_num", "ESN").dropDuplicates("ESN")
    engineDetailDF.createOrReplaceTempView("buildData")
    //    val engineBuildDF = spark.table(Constants.Primary_Engine_Build).createOrReplaceTempView("buildData")
    logger.info("==== read hive table, get build data ====" )

    val  engineMasterDF = spark.read.jdbc(AppConfig.sqlUrl,Constants.Raw_Engine_Base_Detail,prop)
    engineMasterDF.createOrReplaceTempView("masterData")
    logger.info("==== read engine base detail from sqlServer ====")

    val masterBuild = sql(
      s"""
         |select t2.ENGINE_SERIAL_NUM AS ESN, cast(t1.ESN AS string) AS ESN1, t1.ENGINE_PLANT AS engine_plant, t1.ENGINE_PLATFORM AS engine_platform,
         |t1.EMISSION_LEVEL AS emission_level, cast(t1.DISPLACEMENT AS string) AS displacement1,t1.BUILD_DATE AS build_date,
         |t2.TSP AS TSP, t2.VIN AS VIN, t2.ENGINE_MODEL AS engine_model, t2.DISPLACEMENT_STANDARD AS displacement_standard,
         |t2.MANUFACTURING_PLANT AS manufacturing_plant, t2.DISPLACEMENT AS displacement, cast(t2.create_time AS STRING) AS create_time
         |from masterData t2 left outer join buildData t1 on t2.ENGINE_SERIAL_NUM = t1.ESN
         |""".stripMargin)
    sql(s"drop view buildData")
    sql(s"drop view masterData")

    logger.info("==== ready to process column exclude VIN ====")
    val masterFilterRdd = masterBuild.rdd.map{
      item =>
        val esn = if(item.getAs[String]("ESN") == null) item.getAs[String]("ESN1") else item.getAs[String]("ESN")
        val VIN = item.getAs[String]("VIN")
        val TSP = item.getAs[String]("TSP")
        val platform1 =
          if (item.getAs[String]("engine_platform") == null) {
            var engine_mode: String = item.getAs("engine_model")
            var place: String = item.getAs("displacement")
            if(engine_mode == null) {
              engine_mode="Unknown"
            } else if(engine_mode.toUpperCase.equals("ISG")){
              engine_mode="X"
            }
            if(place == null) place = "Unknown"
            engine_mode + place
          } else {
            if(item.getAs[String]("engine_platform").toString.equals("X")){
              "X".concat(item.getAs[String]("displacement1"))
            } else{
              item.getAs[String]("engine_platform")
            }
          }
        val platform = getNormPlatform(platform1)
        var emission_level = if(item.getAs[String]("emission_level") == null)
          item.getAs[String]("displacement_standard") else  item.getAs[String]("emission_level")
        if(emission_level != null){
          emission_level.trim.toUpperCase match {
            case "T3" | "Tier3" => emission_level = "CS3"
            case "T4" => emission_level = "CS4"
            case "E3" | "NS3" | "国三" => emission_level = "NSIII"
            case "E4" | "NS4" | "国四" => emission_level = "NSIV"
            case "JingV" | "JingV++" | "京V" | "E5" | "NS5" | "S5" | "国五" => emission_level = "NSV"
            case "E6" | "NS6" | "国六" | "欧六" => emission_level = "NSVI"
            case "" | "EPA2007" | "无" => emission_level = "Unknown"
            case _ => emission_level
          }
        } else {
          emission_level = "Unknown"
        }
        val plant = if(item.getAs[String]("engine_plant") == null) {
          if(item.getAs[String]("manufacturing_plant") == null) "Unknown"
          else item.getAs[String]("manufacturing_plant")
        } else item.getAs[String]("engine_plant")
        val displacement =  if (item.getAs[String]("displacement1") == null) {
          if((item.getAs[String]("displacement") != null) && item.getAs[String]("displacement").equals("ISG")) {
            "X"
          } else {
            item.getAs[String]("displacement")
          }
        } else {
          if(!item.getAs[String]("displacement1").toString.equals("X")){
            val pattern = new Regex("[^a-zA-Z]")
            (pattern findAllIn item.getAs[String]("displacement1")).mkString("")
          } else{
            item.getAs[String]("displacement1")
          }
        }
        val build_date = if(item.getAs[Date]("build_date") == null) "" else new SimpleDateFormat("yyyy-MM-dd").format(item.getAs[Date]("build_date"))
        val engine_name = if(platform.contains("Unknown")) { "Unknown" } else {platform.toString + "_" + emission_level + "_" + plant}
        val create_time = item.getAs[String]("create_time")
        (esn,VIN,TSP,platform,emission_level,plant,displacement,engine_name,build_date,create_time)

    }

    logger.info("==== deal VIN, according the VIN-Mapping ==== ")
    import spark.implicits._
    //增加新的分区字段
    val partition = functions.udf((engineName: String,build_date: String) => {
      if(engineName.contains("Unknown") || build_date.isEmpty){
        "others"
      } else if(engineName.contains("NSV") || engineName.contains("NSVI")) {
        engineName match {
          case "B6.2_NSVI_CCEC" | "X12_NSV_DCEC" | "D6.7_NSVI_BFCEC" | "X12_NSV_XCEC" | "X12_NSV_ACPL" | "F4.5_NSV_DCEC" => "others"
          case "F4.5_NSV_XCEC" | "B6.7_NSV_CCEC" | "D6.7_NSV_BFCEC" | "D6.7_NSVI_CCEC" | "D4.5_NSVI_GCIC" | "B6.2_NSVI_GCIC" => "others"
          case _ => engineName.toString
        }
      } else {
        "others"
      }
    })
    val esnMsterDF = dealVinMapping(sc,filePath,masterFilterRdd).toDF("ESN","TSP","PLATFORM","EMISSION_LEVEL","PLANT","DISPLACEMENT","ENGINE_NAME","BUILD_DATE","CREATE_TIME",
      "VIN_OEM","VIN_VECHICLE_TYPE","VIN_GROSS_WEIGHT_LOWER","VIN_GROSS_WEIGHT_UPPER","VIN_CAB_TYPE","VIN_DRIVING_TYPE",
      "VIN_POWER_RANGE","VIN_WHEELBASE_LOWER","VIN_WHEELBASE_UPPER","VIN_BUILD_YEAR","VIN_MFG_PLANT").withColumn("PARTITION",partition('ENGINE_NAME,'BUILD_DATE))

    esnMsterDF.createOrReplaceTempView("esnMaster")
    //    logger.info("==== process trim data, add columns to the esnMaster ====")
    //
    //    val cal = Calendar.getInstance()
    //    var trim_date = ""
    //    if(cal.get(Calendar.HOUR_OF_DAY) > 8){
    //      trim_date = new SimpleDateFormat("yyyy-MM-dd").format(cal.add(Calendar.DATE, -1))
    //    } else {
    //      trim_date = new SimpleDateFormat("yyyy-MM-dd").format(cal)
    //    }
    //
    //    val trimDataDF = sql(
    //      s"""
    //      |select ESN,Advertised_Engine_Power,Peak_Torque,LSI_Breakpoint_Speed,HSG_Breakpoint_Speed_1,Engine_CPL,Rear_Axle_Ratio_Low,Tire_Size
    //      |from ${Constants.primary_trim_data} where `date` = ${trim_date}
    //      |""".stripMargin)
    //
    //    trimDataDF.createOrReplaceTempView("trimData")

    logger.info("==== data process end, load data into hive table ====")
    sql(s"insert overwrite table ${Constants.Primary_EsnMaster} select * from esnMaster")

    //    sql(s"""
    //      |insert overwrite table ${Constants.Primary_EsnMaster}
    //      |select t1.*, t2.Advertised_Engine_Power AS AdvertisedEnginePower, t2.Peak_Torque AS PeakTorque,
    //      |t2.LSI_Breakpoint_Speed AS Low_Idle_Speed, t2.HSG_Breakpoint_Speed_1 AS High_Idle_Speed,
    //      |t2.Engine_CPL AS CPL_Number, t2.Rear_Axle_Ratio_Low AS Rear_Axle_Ratio, t2.Tire_Size
    //      | from esnMaster t1 left outer join trimData t2 on t1.ESN = t2.ESN
    //      |""".stripMargin)
    //
    //
    //    sql(s"drop view trimData")
    sql(s"drop view esnMaster")

    //    esnMsterDF.write.mode(SaveMode.Overwrite).saveAsTable(Constants.Primary_EsnMaster)

    //    esnMsterDF.repartition(1).write.format("com.databricks.spark.csv")
    //      .option("header", "true")//在csv第一行有属性"true"，没有就是"false"
    //      .option("delimiter",",")//默认以","分割
    //      .save("C:\\Software\\Data\\EsnMasterTest")

    //将数据同步一份至Digital的sqlServer

    val prop1 = new Properties()
    prop1.setProperty("driver", AppConfig.sqlDriver)
    prop1.setProperty("user", AppConfig.destSqlUser)
    prop1.setProperty("password", AppConfig.destSqlPW)

    val conn = DriverManager.getConnection(AppConfig.destSqlUrl,prop1)
    val sm = conn.prepareCall("truncate table " + Constants.Table_EsnMaster)
    sm.execute()
    sm.close()


    esnMsterDF.write.format ("jdbc")
      .format ("jdbc")
      .option("driver", AppConfig.sqlDriver)
      .option ("url", AppConfig.destSqlUrl)
      .option ("dbtable", Constants.Table_EsnMaster)
      .option ("user", AppConfig.destSqlUser)
      .option ("password", AppConfig.destSqlPW)
      .mode (SaveMode.Append)
      .save ()

  }


  /**
   * 转换异常的platform值
   * @param engine_platform    表中的platform
   * @return
   */
  def getNormPlatform(engine_platform: String): String ={
    if (engine_platform != null) {
      engine_platform.trim.toUpperCase match {
        case "ISD4.0" => "D4.0"
        case "ISD4.5" => "D4.5"
        case "ISD6.7" => "D6.7"
        case "QSB6.7Tier4Final" => "QSB6.7"
        case "QSLTier4Final" => "QSL"
        case "QSM10.8" => "QSM11"
        case "B6.7e5" => "B6.7"
        case "ISB4.0" => "B4.0"
        case "ISB4.5" => "B4.5"
        case "ISB5.9" => "B5.9"
        case "ISB6.2" => "B6.2"
        case "ISB6.7" => "B6.7"
        case "ISC8.3" => "C8.3"
        case "ISF3.8" => "F3.8"
        case "ISF2.8" => "F2.8"
        case "ISF4.5" => "F4.5"
        case "ISL8.9" => "L8.9"
        case "ISL9.5" => "L9.5"
        case "ISM10.8" | "ISM10.9" => "M11"
        case "X10.5" | "X11.0" => "X11"
        case "X11.8" | "X12.0" => "X12"
        case "X13.0" => "X13"
        case "ISZ14.0" => "Z14"
        case "ISZ13.0" | "ISZ13" => "Z13"
        case "IS43.0" | "ISF." | "" | "X1T." => "Unknown"
        case _ => engine_platform.toString
      }
    } else {
      "Unknown"
    }
  }


  /**
   * 处理VIN 的映射关系
   * @param sc
   * @param filePath
   * @param master
   * @return
   */
  def dealVinMapping(sc: SparkContext,filePath:String,master:RDD[(String, String, String, String, String, String, String, String, String, String)]): RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] ={
    logger.info("==== read VIN Mapping, ready to process VIN ====")
    //    val vinMappingRdd = sc.textFile("C:\\Software\\Data\\workspace-gitlab\\hbdata-labeling-job\\src\\main\\resources\\VIN-Mapping.csv")
    val vinMappingRdd = sc.textFile(filePath + "/VIN-Mapping.csv")
      .map{
        item =>
          val fields = item.split(",",-1)
          (fields(0) + "-" + fields(2) + "-" + fields(3),fields(4),fields(5),fields(6))
      }

    val vinNumMappingRdd = vinMappingRdd.map{
      str =>
        val i = str._1.lastIndexOf("-")
        (str._1.substring(0,i), str._2)
    }.distinct()

    val broadcastval = sc.broadcast(vinMappingRdd.collect())
    val broadcast2: Broadcast[Array[(String, String)]] = sc.broadcast(vinNumMappingRdd.collect())

    //    vinMappingRdd.take(10).foreach(println)
    logger.info("==== process VIN, split and map column ====")
    val EsnMasterRdd = master.map{
      //(esn,VIN,TSP,platform,emission_level,plant,displacement,engine_name,build_date,create_time)
      item =>
        val VIN = item._2
        //        var VIN_OEM,VIN_Vechicle_Type,VIN_Gross_Weight_Lower,VIN_Gross_Weight_Upper,VIN_Cab_Type,VIN_Driving_Type,VIN_Power_Range,
        //            VIN_Wheelbase_Lower,VIN_Wheelbase_Upper,VIN_Build_Year,VIN_MFG_Plant = ""
        val obj = new esnMasterObj("","","","","","","","","","","")
        if ((VIN != null) && (VIN.length == 17)){
          val vinMap: Array[String] = splitVIN(VIN.toString)
          vinMap.foreach{
            vinStr => {
              val filterMapping = broadcastval.value.filter(_._1.equals(vinStr))
              if(!filterMapping.isEmpty){
                filterMapping.map{
                  item1 =>
                    item1._2 match {
                      case "OEM" =>  obj.VIN_OEM = item1._3
                      case "车型" =>  obj.VIN_Vechicle_Type = item1._3
                      case "总重" =>  obj.VIN_Gross_Weight_Lower = item1._3
                        obj.VIN_Gross_Weight_Upper = item1._4
                      case "驾驶室" =>  obj.VIN_Cab_Type = item1._3
                      case "驱动" =>  obj.VIN_Driving_Type = item1._3
                      case "发动机功率范围" =>  obj.VIN_Power_Range = item1._3
                      case "轴距" =>  obj.VIN_Wheelbase_Lower = item1._3
                        obj.VIN_Wheelbase_Upper = item1._4
                      case "生产年份" =>  obj.VIN_Build_Year = item1._3
                      case "装配厂" =>  obj.VIN_MFG_Plant = item1._3
                      case _ => fillBlankFields(broadcast2,vinStr,obj)
                    }
                }
              } else {
                fillBlankFields(broadcast2,vinStr,obj)
              }
            }
          }
        }
        (item._1,item._3,item._4,item._5,item._6,item._7,item._8,item._9,item._10,obj.VIN_OEM,obj.VIN_Vechicle_Type,obj.VIN_Gross_Weight_Lower,obj.VIN_Gross_Weight_Upper,
          obj.VIN_Cab_Type,obj.VIN_Driving_Type,obj.VIN_Power_Range, obj.VIN_Wheelbase_Lower,obj.VIN_Wheelbase_Upper,obj.VIN_Build_Year,obj.VIN_MFG_Plant)
    }
    EsnMasterRdd
  }

  /**
   * 将VIN拆分组成mapping表中的位数对应关系
   * @param VIN    VIN
   * @return
   */
  def splitVIN(VIN:String): Array[String] ={
    import scala.collection.mutable.ArrayBuffer
    var vinMap = new ArrayBuffer[String]
    val oemStr = VIN.substring(0,3)
    val leftStr = VIN.toCharArray
    //    logger.info("==== split VIN and convert to VInMapping relation === ")
    vinMap += (oemStr + "-" + "123" + "-" + oemStr)
    for( i <- 3 until leftStr.length){
      // 过滤特别的生产时间
      if(i == 9){
        if(oemStr.equals("LGA") || oemStr.equals("LRD") || oemStr.equals("LJ1")){
          vinMap += (oemStr + "-" + String.valueOf(i+1) + "-" + leftStr(i).toString)
        } else {
          vinMap += ("-" + String.valueOf(i+1) + "-" + leftStr(i).toString)
        }
      } else {
        vinMap += (oemStr + "-" + String.valueOf(i+1) + "-" + leftStr(i).toString)
      }
    }
    vinMap.toArray
  }

  /**
   * 将无法解析的VIN字母填充到相应的字段
   * @param broadcast        解析VIN字段
   * @param str              主数据中的VIN解析后的字串
   */
  def fillBlankFields(broadcast: Broadcast[Array[(String, String)]], str: String, obj:esnMasterObj) : Unit={
    val i = str.lastIndexOf("-")
    val vinStr = str.substring(0, i)
    val filterType = broadcast.value.filter(_._1.equals(vinStr))
    if(filterType.isEmpty){
      obj.VIN_OEM = vinStr.split("-")(0)
    }else {
      filterType.foreach{
        item =>
          item._2 match {
            case "OEM" => obj.VIN_OEM = str.substring(i+1,str.length)
            case "车型" =>  obj.VIN_Vechicle_Type = str.substring(i+1,str.length)
            case "总重" =>  obj.VIN_Gross_Weight_Lower = str.substring(i+1,str.length)
              obj.VIN_Gross_Weight_Upper = str.substring(i+1,str.length)
            case "驾驶室" =>  obj.VIN_Cab_Type = str.substring(i+1,str.length)
            case "驱动" =>  obj.VIN_Driving_Type = str.substring(i+1,str.length)
            case "发动机功率范围" =>  obj.VIN_Power_Range = str.substring(i+1,str.length)
            case "轴距" =>  obj.VIN_Wheelbase_Lower = str.substring(i+1,str.length)
              obj.VIN_Wheelbase_Upper = str.substring(i+1,str.length)
            case "生产年份" =>  obj.VIN_Build_Year = str.substring(i+1,str.length)
            case "装配厂" =>  obj.VIN_MFG_Plant = str.substring(i+1,str.length)
            case _ => "others"
          }
      }
    }
  }


}


class esnMasterObj(vinOem: String,vechicleType: String,grossWeightLower: String,grossWeightUpper: String,cabType: String, drivingType: String,
                   powerRange: String, wheelbaseLower: String,wheelbaseUpper: String,buildYear: String,MFG_Plant: String){
  var VIN_OEM: String = vinOem
  var VIN_Vechicle_Type: String = vechicleType
  var VIN_Gross_Weight_Lower: String = grossWeightLower
  var VIN_Gross_Weight_Upper: String = grossWeightUpper
  var VIN_Cab_Type: String = cabType
  var VIN_Driving_Type: String = drivingType
  var VIN_Power_Range: String = powerRange
  var VIN_Wheelbase_Lower: String = wheelbaseLower
  var VIN_Wheelbase_Upper: String = wheelbaseUpper
  var VIN_Build_Year: String = buildYear
  var VIN_MFG_Plant: String = MFG_Plant

}