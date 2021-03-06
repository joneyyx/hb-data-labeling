package com.cummins.dataLabeling.test


import com.cummins.dataLabeling.common.constants.Constants
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.math.pow

object TelHBLabledPrimaryJob extends Serializable {

  val logger = Logger.getLogger(this.getClass)

  def exec(spark: SparkSession, tsp: String, beijingYesterday: String) = {
    import spark._
    import spark.implicits._

    //获取北京时间昨天 2019-11-27 -> 20191127
    val yesterday = beijingYesterday.split("-")(0) + beijingYesterday.split("-")(1) + beijingYesterday.split("-")(2)

    //获取 tsp-report_date数据
    spark.read.orc(s"wasbs://westlake@dldevblob4qastorage.blob.core.chinacloudapi.cn/westlake_data/raw/telematics_hb_data/HB_PROCESSED_DATA_DAY/$tsp/20200102/*").createOrReplaceTempView("hb_rawdata")
     sql(
        s"""
           |select
           |h5 as `Telematics_Partner_Name`,
           |h6 as `Occurrence_Date_Time`,
           |h30 as `TSP_NAME`,
           |h43 as `Equipment_ID`,
           |h44 as `VIN`,
           |h53 as `Telematics_Box_ID`,
           |h54 as `Telematics_Box_Hardware_Variant`,
           |h55 as `Telematics_Box_Software_Version`,
           |h56 as `Latitude`,
           |h57 as `Longitude`,
           |h58 as `Altitude`,
           |h59 as `Direction_Heading`,
           |h60 as `Direction_Heading_Orientation`,
           |h62 as `Direction_Heading_Degree`,
           |h63 as `GPS_Vehicle_Speed`,
           |h64 as `Engine_Serial_Number`,
           |h70['pc1'] as `A/C_High_Pressure_Fan_Switch`,
           |h70['pc2'] as `Brake_Switch`,
           |h70['pc3'] as `Clutch_Switch`,
           |h70['pc4'] as `Cruise_Control_Enable_Switch`,
           |h70['pc5'] as `Parking_Brake_Switch`,
           |h70['pc6'] as `PTO_Governor_State`,
           |h70['pc7'] as `Water_In_Fuel_Indicator_1`,
           |h70['pc8'] as `Calibration_Identification`,
           |h70['pc9'] as `Calibration_Verification_Number`,
           |h70['pc10'] as `Number_of_Software_Identification_Fields`,
           |h70['pc11'] as `Software_Identification`,
           |h70['pc12'] as `Make`,
           |h70['pc13'] as `Model`,
           |h70['pc14'] as `Unit_Number_(Power_Unit)`,
           |h70['pc15'] as `Engine_Operating_State`,
           |h70['pc16'] as `Engine_Torque_Mode`,
           |h70['pc17'] as `Engine_Amber_Warning_Lamp_Command`,
           |h70['pc18'] as `Engine_Red_Stop_Lamp_Command`,
           |h70['pc19'] as `OBD_Malfunction_Indicator_Lamp_Command`,
           |h70['pc20'] as `Aftertreatment_1_Intake_Dew_Point`,
           |h70['pc21'] as `Aftertreatment_1_Exhaust_Dew_Point`,
           |h70['pc22'] as `Accelerator_Interlock_Switch`,
           |h70['pc23'] as `DPF_Thermal_Management_Active`,
           |h70['pc24'] as `Cruise_Control_Active`,
           |h70['pc25'] as `Fan_Drive_State`,
           |h70['pc26'] as `Diesel_Particulate_Filter_Status`,
           |h70['pc27'] as `SCR_Thermal_Management_Active`,
           |h70['pc28'] as `Aftertreatment_1_SCR_System_State`,
           |h70['pc29'] as `Aftertreatment_SCR_Operator_Inducement_Severity`,
           |h70['pc30'] as `Diesel_Particulate_Filter_Regeneration_Inhibit_Switch`,
           |h70['pc31'] as `Diesel_Particulate_Filter_Regeneration_Force_Switch`,
           |h71['pn1'] as `Accelerator_Pedal_Position_1`,
           |h71['pn2'] as `Aftertreatment_1_Diesel_Exhaust_Fluid_Tank_Level`,
           |h71['pn3'] as `Aftertreatment_1_Outlet_NOx_1`,
           |h71['pn4'] as `Aftertreatment_1_SCR_Intake_Temperature`,
           |h71['pn5'] as `Aftertreatment_1_SCR_Outlet_Temperature`,
           |h71['pn6'] as `Ambient_Air_Temperature`,
           |h71['pn7'] as `Barometric_Pressure`,
           |h71['pn8'] as `Battery_Potential_/_Power_Input_1`,
           |h71['pn9'] as `Commanded_Engine_Fuel_Rail_Pressure`,
           |h71['pn10'] as `Engine_Coolant_Level_1`,
           |h71['pn11'] as `Engine_Fuel_Rate`,
           |h71['pn12'] as `Engine_Oil_Temperature_1`,
           |h71['pn13'] as `Engine_Speed`,
           |h71['pn14'] as `Engine_Total_Fuel_Used`,
           |h71['pn15'] as `Engine_Total_Hours_of_Operation`,
           |h71['pn16'] as `Total_ECU_Run_Time`,
           |h71['pn17'] as `Wheel-Based_Vehicle_Speed`,
           |h71['pn18'] as `Actual_Engine_-_Percent_Torque_(Fractional)`,
           |h71['pn19'] as `Actual_Maximum_Available_Engine_-_Percent_Torque`,
           |h71['pn20'] as `Engine_Derate_Request`,
           |h71['pn21'] as `Engine_Fan_1_Requested_Percent_Speed`,
           |h71['pn22'] as `Engine_Total_Idle_Fuel_Used`,
           |h71['pn23'] as `Engine_Total_Idle_Hours`,
           |h71['pn24'] as `Engine_Trip_Fuel`,
           |h71['pn25'] as `Fan_Speed`,
           |h71['pn26'] as `Aftertreatment_1_Diesel_Exhaust_Fluid_Tank_Heater`,
           |h71['pn27'] as `Engine_Exhaust_Gas_Recirculation_1_Mass_Flow_Rate`,
           |h71['pn28'] as `Engine_Intake_Air_Mass_Flow_Rate`,
           |h71['pn29'] as `Transmission_Actual_Gear_Ratio`,
           |h71['pn30'] as `Engine_Throttle_Valve_1_Position_1`,
           |h71['pn31'] as `Aftertreatment_1_Diesel_Oxidation_Catalyst_Intake_Gas_Temperature`,
           |h71['pn32'] as `Aftertreatment_1_SCR_Conversion_Efficiency`,
           |h71['pn33'] as `Diesel_Particulate_Filter_1_Ash_Load_Percent`,
           |h71['pn34'] as `Aftertreatment_1_Diesel_Particulate_Filter_Intake_Gas_Temperature`,
           |h71['pn35'] as `Aftertreatment_1_Diesel_Particulate_Filter_Outlet_Gas_Temperature`,
           |h71['pn36'] as `Diesel_Particulate_Filter_Outlet_Pressure_1`,
           |h71['pn37'] as `Aftertreatment_1_SCR_Intake_Nox_1`,
           |h71['pn38'] as `Aftertreatment_1_Diesel_Particulate_Filter_Differential_Pressure`,
           |h71['pn39'] as `Aftertreatment_1_Diesel_Exhaust_Fluid_Actual_Quantity_of_Integrator`,
           |h71['pn40'] as `Engine_Exhaust_Bank_1_Pressure_Regulator_Position`,
           |h71['pn41'] as `Engine_Exhaust_Manifold_Bank_1_Flow_Balance_Valve_Actuator_Control`,
           |h71['pn42'] as `Diesel_Particulate_Filter_1_Soot_Density`,
           |h71['pn43'] as `Aftertreatment_1_Total_Fuel_Used`,
           |h71['pn44'] as `Aftertreatment_1_Diesel_Exhaust_Fluid_Concentration`,
           |h72['pi1'] as `Actual_Engine_-_Percent_Torque`,
           |h72['pi2'] as `Aftertreatment_1_Diesel_Exhaust_Fluid_Tank_Temperature`,
           |h72['pi3'] as `Cruise_Control_Set_Speed`,
           |h72['pi4'] as `Driver's_Demand_Engine_-_Percent_Torque`,
           |h72['pi5'] as `Engine_Coolant_Temperature`,
           |h72['pi6'] as `Engine_Intake_Manifold_#1_Pressure`,
           |h72['pi7'] as `Engine_Intake_Manifold_1_Temperature`,
           |h72['pi8'] as `Engine_Oil_Pressure`,
           |h72['pi9'] as `Engine_Percent_Load_At_Current_Speed`,
           |h72['pi10'] as `Engine_Reference_Torque`,
           |h72['pi11'] as `Nominal_Friction_-_Percent_Torque`,
           |h72['pi12'] as `Time_Since_Engine_Start`,
           |h72['pi13'] as `Engine_Demand_-_Percent_Torque`,
           |h72['pi14'] as `Engine_Total_Revolutions`,
           |h72['pi15'] as `High_Resolution_Total_Vehicle_Distance`,
           |h72['pi16'] as `Gross_Combination_Weight`,
           |h72['pi17'] as `Unfiltered_Raw_Vehicle_Weight`,
           |h72['pi18'] as `Diesel_Particulate_Filter_1_Time_Since_Last_Active_Regeneration`,
           |h72['pi19'] as `Diesel_Exhaust_Fluid_Quality_Malfunction_Time`,
           |h72['pi20'] as `Diesel_Exhaust_Fluid_Tank_1_Empty_Time`,
           |h72['pi21'] as `Engine_Exhaust_Pressure_1`,
           |h72['pi22'] as `SCR_Operator_Inducement_Total_Time`,
           |h72['pi23'] as `Aftertreatment_1_Total_Regeneration_Time`,
           |h72['pi24'] as `Aftertreatment_1_Total_Disabled_Time`,
           |h72['pi25'] as `Aftertreatment_1_Total_Number_of_Active_Regenerations`,
           |h72['pi26'] as `Aftertreatment_1_Diesel_Particulate_Filter_Total_Number_of_Active_Regeneration_Inhibit_Requests`,
           |h72['pi27'] as `Aftertreatment_1_Diesel_Particulate_Filter_Total_Number_of_Active_Regeneration_Manual_Requests`,
           |h72['pi28'] as `Aftertreatment_1_Average_Time_Between_Active_Regenerations`,
           |h72['pi29'] as `Aftertreatment_1_Diesel_Exhaust_Fluid_Doser_Absolute_Pressure`,
           |h73 as `Displacement`,
           |h74 as `Engine_Model`,
           |h75 as `Gradient`,
           |h76 as `Accel_Rate`,
           |h77 as `Fuel_TANK_Level`,
           |h78 as `Rear_Axle_Ratio`,
           |h79 as `Transmission_Model`,
           |h80 as `Tire_Model`
           |from hb_rawdata
           |""".stripMargin)
        .createOrReplaceTempView("hb_data")

    //读取GPSData
    spark.read.orc("/test/ybz/gdb_new/*").createOrReplaceTempView("gps_data")

    //如果tsp为Cty,使用经纬度3位匹配，关联GPS数据获取roadname及判断highcode（低精度）
    //如果tsp为其他，使用经纬度4位匹配，关联highway数据获取roadname及判断highcode（高精度）
//    if("Cty".equals(tsp)){
//      logger.info("第一种")
//      sql(
//        s"""
//           |select t1.*,split(`Occurrence_Date_Time`,'-')[1] as month,
//           |t2.province,t2.city,t2.roadName,t2.province as region,
//           |(case when t2.roadName in('G6京藏高速','G15沈海高速','G30连霍高速','G56杭瑞高速','G25长深高速','G65包茂高速','G5京昆高速','G75兰海高速','G4京港澳高速','G60沪昆高速','G55二广高速','G22青兰高速','G45大广高速','G70福银高速','G20青银高速','G18荣乌高速','G76厦蓉高速','G50沪渝高速','G2京沪高速','G42沪蓉高速','G3京台高速','G35济广高速','G7京新高速','G85渝昆高速','G10绥满高速','G11鹤大高速','G72泉南高速','G40沪陕高速','G1京哈高速','G80广昆高速','G12珲乌高速','G16丹锡高速','G36宁洛高速','G78汕昆高速')
//           | then t2.roadName else 'Others' end) as highway_code
//           |from hb_data t1 left join gps_data t2 on regexp_extract(t1.Latitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lat and regexp_extract(t1.Longitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lgt
//           |""".stripMargin)
//        .withColumn("Landscape",getLandscape($"Altitude",$"Barometric_Pressure"))
//        .withColumn("Climate_Zone",getClimateZone($"province",$"city",$"month"))
//        .withColumn("report_date",lit("20200103"))
//
//        .createOrReplaceTempView("clean_data")

//    } else {
//      logger.info("第二种")
      sql(
        """
          |select t1.*,split(`Occurrence_Date_Time`,'-')[1] as month,
          |t2.province,t2.city,t2.province as region
          |from hb_data t1 left join gps_data t2 on regexp_extract(t1.Latitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lat and regexp_extract(t1.Longitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lgt
          |""".stripMargin)
        .createOrReplaceTempView("hb_province_data")

      //读取highway数据
//      val highwaySchemaType = StructType(Array(
//        StructField("lgt", StringType, true),
//        StructField("lat", StringType, true),
//        StructField("roadname", StringType, true),
//        StructField("high_resolution", StringType, true)
//      ))
//      spark.read.schema(highwaySchemaType).csv("/test/ybz/highway.csv").createOrReplaceTempView("highway_data")
      sql(
        """
          |select t1.* ,t2.roadname,
          |(case when t2.roadName in('G6京藏高速','G15沈海高速','G30连霍高速','G56杭瑞高速','G25长深高速','G65包茂高速','G5京昆高速','G75兰海高速','G4京港澳高速','G60沪昆高速','G55二广高速','G22青兰高速','G45大广高速','G70福银高速','G20青银高速','G18荣乌高速','G76厦蓉高速','G50沪渝高速','G2京沪高速','G42沪蓉高速','G3京台高速','G35济广高速','G7京新高速','G85渝昆高速','G10绥满高速','G11鹤大高速','G72泉南高速','G40沪陕高速','G1京哈高速','G80广昆高速','G12珲乌高速','G16丹锡高速','G36宁洛高速','G78汕昆高速')
          | then t2.roadName else 'Others' end) as highway_code
          |from hb_province_data t1 left join det_dev_reference.highway_data_new t2 on regexp_extract(t1.Latitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lat and regexp_extract(t1.Longitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lgt
          |""".stripMargin)
        .withColumn("Landscape",getLandscape($"Altitude",$"Barometric_Pressure"))
        .withColumn("Climate_Zone", getClimateZone($"province", $"city", $"month"))
        .withColumn("report_date",lit("20200104"))
//        .drop("month")
        .createOrReplaceTempView("clean_data")
////        .write.format("Hive").mode(SaveMode.Append)
////        .partitionBy("report_date")
////        .bucketBy(3,"Engine_Serial_Number")
////        .saveAsTable("test.hblabeling3")
//    }

    // TODO 关联master获取engine_name字段
    //    sql(
    //      """
    //        |select t1.*,t2.engine_name
    //        |from clean_data t1 join primary.esnmaster t2 on t1.Engine_Serial_Number = t2.esn
    //        |""".stripMargin)
    //        .createOrReplaceTempView("res_data")

    // TODO 写入hive表
    sql(
      """
        |insert into table test.hblabeling3 partition(`report_date`) select
        |`Telematics_Partner_Name`,
        |`Occurrence_Date_Time`,
        |`TSP_NAME`,
        |`Equipment_ID`,
        |`VIN`,
        |`Telematics_Box_ID`,
        |`Telematics_Box_Hardware_Variant`,
        |`Telematics_Box_Software_Version`,
        |`Latitude`,
        |`Longitude`,
        |`Altitude`,
        |`Direction_Heading`,
        |`Direction_Heading_Orientation`,
        |`Direction_Heading_Degree`,
        |`GPS_Vehicle_Speed`,
        |`Engine_Serial_Number`,
        |`A/C_High_Pressure_Fan_Switch`,
        |`Brake_Switch`,
        |`Clutch_Switch`,
        |`Cruise_Control_Enable_Switch`,
        |`Parking_Brake_Switch`,
        |`PTO_Governor_State`,
        |`Water_In_Fuel_Indicator_1`,
        |`Calibration_Identification`,
        |`Calibration_Verification_Number`,
        |`Number_of_Software_Identification_Fields`,
        |`Software_Identification`,
        |`Make`,
        |`Model`,
        |`Unit_Number_(Power_Unit)`,
        |`Engine_Operating_State`,
        |`Engine_Torque_Mode`,
        |`Engine_Amber_Warning_Lamp_Command`,
        |`Engine_Red_Stop_Lamp_Command`,
        |`OBD_Malfunction_Indicator_Lamp_Command`,
        |`Aftertreatment_1_Intake_Dew_Point`,
        |`Aftertreatment_1_Exhaust_Dew_Point`,
        |`Accelerator_Interlock_Switch`,
        |`DPF_Thermal_Management_Active`,
        |`Cruise_Control_Active`,
        |`Fan_Drive_State`,
        |`Diesel_Particulate_Filter_Status`,
        |`SCR_Thermal_Management_Active`,
        |`Aftertreatment_1_SCR_System_State`,
        |`Aftertreatment_SCR_Operator_Inducement_Severity`,
        |`Diesel_Particulate_Filter_Regeneration_Inhibit_Switch`,
        |`Diesel_Particulate_Filter_Regeneration_Force_Switch`,
        |`Accelerator_Pedal_Position_1`,
        |`Aftertreatment_1_Diesel_Exhaust_Fluid_Tank_Level`,
        |`Aftertreatment_1_Outlet_NOx_1`,
        |`Aftertreatment_1_SCR_Intake_Temperature`,
        |`Aftertreatment_1_SCR_Outlet_Temperature`,
        |`Ambient_Air_Temperature`,
        |`Barometric_Pressure`,
        |`Battery_Potential_/_Power_Input_1`,
        |`Commanded_Engine_Fuel_Rail_Pressure`,
        |`Engine_Coolant_Level_1`,
        |`Engine_Fuel_Rate`,
        |`Engine_Oil_Temperature_1`,
        |`Engine_Speed`,
        |`Engine_Total_Fuel_Used`,
        |`Engine_Total_Hours_of_Operation`,
        |`Total_ECU_Run_Time`,
        |`Wheel-Based_Vehicle_Speed`,
        |`Actual_Engine_-_Percent_Torque_(Fractional)`,
        |`Actual_Maximum_Available_Engine_-_Percent_Torque`,
        |`Engine_Derate_Request`,
        |`Engine_Fan_1_Requested_Percent_Speed`,
        |`Engine_Total_Idle_Fuel_Used`,
        |`Engine_Total_Idle_Hours`,
        |`Engine_Trip_Fuel`,
        |`Fan_Speed`,
        |`Aftertreatment_1_Diesel_Exhaust_Fluid_Tank_Heater`,
        |`Engine_Exhaust_Gas_Recirculation_1_Mass_Flow_Rate`,
        |`Engine_Intake_Air_Mass_Flow_Rate`,
        |`Transmission_Actual_Gear_Ratio`,
        |`Engine_Throttle_Valve_1_Position_1`,
        |`Aftertreatment_1_Diesel_Oxidation_Catalyst_Intake_Gas_Temperature`,
        |`Aftertreatment_1_SCR_Conversion_Efficiency`,
        |`Diesel_Particulate_Filter_1_Ash_Load_Percent`,
        |`Aftertreatment_1_Diesel_Particulate_Filter_Intake_Gas_Temperature`,
        |`Aftertreatment_1_Diesel_Particulate_Filter_Outlet_Gas_Temperature`,
        |`Diesel_Particulate_Filter_Outlet_Pressure_1`,
        |`Aftertreatment_1_SCR_Intake_Nox_1`,
        |`Aftertreatment_1_Diesel_Particulate_Filter_Differential_Pressure`,
        |`Aftertreatment_1_Diesel_Exhaust_Fluid_Actual_Quantity_of_Integrator`,
        |`Engine_Exhaust_Bank_1_Pressure_Regulator_Position`,
        |`Engine_Exhaust_Manifold_Bank_1_Flow_Balance_Valve_Actuator_Control`,
        |`Diesel_Particulate_Filter_1_Soot_Density`,
        |`Aftertreatment_1_Total_Fuel_Used`,
        |`Aftertreatment_1_Diesel_Exhaust_Fluid_Concentration`,
        |`Actual_Engine_-_Percent_Torque`,
        |`Aftertreatment_1_Diesel_Exhaust_Fluid_Tank_Temperature`,
        |`Cruise_Control_Set_Speed`,
        |`Driver's_Demand_Engine_-_Percent_Torque`,
        |`Engine_Coolant_Temperature`,
        |`Engine_Intake_Manifold_#1_Pressure`,
        |`Engine_Intake_Manifold_1_Temperature`,
        |`Engine_Oil_Pressure`,
        |`Engine_Percent_Load_At_Current_Speed`,
        |`Engine_Reference_Torque`,
        |`Nominal_Friction_-_Percent_Torque`,
        |`Time_Since_Engine_Start`,
        |`Engine_Demand_-_Percent_Torque`,
        |`Engine_Total_Revolutions`,
        |`High_Resolution_Total_Vehicle_Distance`,
        |`Gross_Combination_Weight`,
        |`Unfiltered_Raw_Vehicle_Weight`,
        |`Diesel_Particulate_Filter_1_Time_Since_Last_Active_Regeneration`,
        |`Diesel_Exhaust_Fluid_Quality_Malfunction_Time`,
        |`Diesel_Exhaust_Fluid_Tank_1_Empty_Time`,
        |`Engine_Exhaust_Pressure_1`,
        |`SCR_Operator_Inducement_Total_Time`,
        |`Aftertreatment_1_Total_Regeneration_Time`,
        |`Aftertreatment_1_Total_Disabled_Time`,
        |`Aftertreatment_1_Total_Number_of_Active_Regenerations`,
        |`Aftertreatment_1_Diesel_Particulate_Filter_Total_Number_of_Active_Regeneration_Inhibit_Requests`,
        |`Aftertreatment_1_Diesel_Particulate_Filter_Total_Number_of_Active_Regeneration_Manual_Requests`,
        |`Aftertreatment_1_Average_Time_Between_Active_Regenerations`,
        |`Aftertreatment_1_Diesel_Exhaust_Fluid_Doser_Absolute_Pressure`,
        |`Displacement` ,
        |`Engine_Model` ,
        |`Gradient` ,
        |`Accel_Rate` ,
        |`Fuel_TANK_Level` ,
        |`Rear_Axle_Ratio` ,
        |`Transmission_Model` ,
        |`Tire_Model` ,
        |`province` ,
        |`city` ,
        |`roadName` ,
        |`region` ,
        |`highway_code` ,
        |`Landscape` ,
        |`Climate_Zone`,
        |`report_date`
        |from clean_data
        |""".stripMargin)

//    sql(
//      """
//        |insert into table test.labeling partition(`report_date`) select Landscape,report_date from clean_data
//        |""".stripMargin)

  }

  def getLandscape = udf {
    //, Barometric_Pressure: Double
    (Altitude:String,Barometric_Pressure:String) =>
      var value = ""
      var altitude= -1.0
      if (Altitude != null&& !"".equals(Altitude)){
        altitude =  Altitude.toInt
        //&& Barometric_Pressure !="" && (Altitude.equals("")||Altitude == null)
      }else if (Barometric_Pressure != null&& !"null".equals(Barometric_Pressure)&& !"".equals(Barometric_Pressure) && (Barometric_Pressure.toDouble<=101.5)){
        altitude = (44300*(1-pow((Barometric_Pressure.toDouble/101.325),1/5.256)))
      }
      if (altitude >= 0   && altitude <= 200){value= "Plain" }
      else if ( 200  < altitude && altitude <=  500 ){value= "Hills" }
      else if ( 500  < altitude && altitude <=  1000 ){value= "Mountain_Area" }
      else if ( 1000  < altitude && altitude <=  2400 ){value= "Plateau" }
      else if ( 2400  < altitude){value= "High_Plateau" }
      value
  }

  def getClimateZone = udf {
    (province:String,city:String,month:String) =>
      var value= "Others"
      if (Constants.region.contains(province)){
        if (Constants.province_Xinjiang.equals(province)&&Constants.city_Tulufan.equals(city)&& Constants.month_dry.contains(month)){value="Hot_Dry_Area"}
        else if (Constants.province_Hainan.equals(province)&&Constants.month_humid.contains(month)){value="Hot_Humid_Area"}
        else if (Constants.province_HeiQingXi.contains(province)&&Constants.month_cold.contains(month)){value="Cold_Region"}
        else if (Constants.province_InnerMongolia.equals(province)&&Constants.city_InnerMongolia.contains(city)&&Constants.month_cold.contains(month)){value="Cold_Region"}
        else if (Constants.province_Gansu.equals(province)&&Constants.city_Gansu.contains(city)&&Constants.month_cold.contains(month)){value="Cold_Region"}
        else if (Constants.province_Sichuan.equals(province)&&Constants.city_Sichuan.contains(city)&&Constants.month_cold.contains(month)){value="Cold_Region"}
        else if (Constants.province_Jilin.equals(province)&&Constants.city_Jilin.contains(city)&&Constants.month_cold.contains(month)){value="Cold_Region"}
        else if (Constants.province_Xinjiang.equals(province)&&Constants.city_Xinjiang.contains(city)&&Constants.month_cold.contains(month)){value="Cold_Region"}
      }
      value
  }

}



