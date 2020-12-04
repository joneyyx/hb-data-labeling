package com.cummins.dataLabeling.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.math.pow

object TestTelHBLabledPrimaryJobCty extends Serializable {

  val region : List[String] = List("新疆维吾尔自治区","海南省","黑龙江省","青海省","西藏自治区","内蒙古自治区","甘肃省","四川省","吉林省","新疆维吾尔自治区")

  val province_Xinjiang = "新疆维吾尔自治区"
  val province_Hainan = "海南省"
  val province_HeiQingXi : List[String] = List("黑龙江省","青海省","西藏自治区")
  val province_InnerMongolia = "内蒙古自治区"
  val province_Gansu = "甘肃省"
  val province_Sichuan = "四川省"
  val province_Jilin = "吉林省"

  val city_Tulufan ="吐鲁番市"
  val city_InnerMongolia : List[String] = List("巴彦淖尔市","赤峰市","呼伦贝尔市","兴安盟","乌兰察布市","锡林郭勒盟","呼和浩特市","包头市")
  val city_Gansu : List[String] = List("兰州市","庆阳市","陇南市","天水市","甘南藏族自治州","临夏回族自治州","平凉市","定西市","白银市")
  val city_Sichuan : List[String] = List("甘孜藏族自治州","阿坝藏族羌族自治州")
  val city_Jilin : List[String] = List("通化市","延边朝鲜族自治州","松原市","白山市","白城市")
  val city_Xinjiang : List[String] = List("阿勒泰地区","伊犁哈萨克自治州","克孜勒苏柯尔克孜自治州","克拉玛依市","哈密市","喀什地区")
  val month_cold : List[String] = List("12","01","02")
  val month_humid : List[String] = List("05","06","07","08","09")
  val month_dry : List[String] = List("06","07","08")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("sparkAppName")
      .master("local")
//            .config("spark.sql.warehouse.dir", warehouseloaction)
      //      .enableHiveSupport()
      .getOrCreate()
    exec(spark,"DF","20191230")
  }

  //, tsp: String, report_date: String
  def exec(spark: SparkSession, tsp: String, report_date: String) = {
    import spark.implicits._
    import spark._

    //获取 tsp-report_date数据
    spark.read.orc(s"C:/Users/rl895/Desktop/ybz/ecuruntime/$report_date/*").createOrReplaceTempView("hb_rawdata")
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
    spark.read.orc("C:/Users/rl895/Desktop/ybz/ssap/gdb_new/*").createOrReplaceTempView("gps_data")
    if("Cty".equals(tsp)){
      println("111111111111111")
      sql(
        s"""
           |select t1.*,split(`Occurrence_Date_Time`,'-')[1] as month,
           |t2.province,t2.city,t2.roadName,t2.province as region,
           |(case when t2.roadName in('G6京藏高速','G15沈海高速','G30连霍高速','G56杭瑞高速','G25长深高速','G65包茂高速','G5京昆高速','G75兰海高速','G4京港澳高速','G60沪昆高速','G55二广高速','G22青兰高速','G45大广高速','G70福银高速','G20青银高速','G18荣乌高速','G76厦蓉高速','G50沪渝高速','G2京沪高速','G42沪蓉高速','G3京台高速','G35济广高速','G7京新高速','G85渝昆高速','G10绥满高速','G11鹤大高速','G72泉南高速','G40沪陕高速','G1京哈高速','G80广昆高速','G12珲乌高速','G16丹锡高速','G36宁洛高速','G78汕昆高速')
           | then t2.roadName else 'Others' end) as highway_code
           |from hb_data t1 left join gps_data t2 on regexp_extract(t1.Latitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lat and regexp_extract(t1.Longitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lgt
           |""".stripMargin)
        .withColumn("Landscape",getLandscape($"Altitude",$"Barometric_Pressure"))
        .withColumn("Climate_Zone",getClimateZone($"province",$"city",$"month"))
        .show()
//        .createOrReplaceTempView("clean_data")

    } else {
      println("22222222222222")
      sql(
        """
          |select t1.*,split(`Occurrence_Date_Time`,'-')[1] as month,
          |t2.province,t2.city,t2.province as region
          |from hb_data t1 left join gps_data t2 on regexp_extract(t1.Latitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lat and regexp_extract(t1.Longitude,'([0-9]*.[0-9][0-9][0-9])') = t2.lgt
          |""".stripMargin)
        .createOrReplaceTempView("hb_province_data")

      //读取highway数据
      val highwaySchemaType = StructType(Array(
        StructField("lgt", StringType, true),
        StructField("lat", StringType, true),
        StructField("roadname", StringType, true),
        StructField("high_resolution", StringType, true)
      ))
      spark.read.schema(highwaySchemaType).csv("C:/Users/rl895/Desktop/ybz/ssap/highway.csv").createOrReplaceTempView("highway_data")
      sql(
        """
          |select t1.* ,t2.roadname,
          |(case when t2.roadName in('G6京藏高速','G15沈海高速','G30连霍高速','G56杭瑞高速','G25长深高速','G65包茂高速','G5京昆高速','G75兰海高速','G4京港澳高速','G60沪昆高速','G55二广高速','G22青兰高速','G45大广高速','G70福银高速','G20青银高速','G18荣乌高速','G76厦蓉高速','G50沪渝高速','G2京沪高速','G42沪蓉高速','G3京台高速','G35济广高速','G7京新高速','G85渝昆高速','G10绥满高速','G11鹤大高速','G72泉南高速','G40沪陕高速','G1京哈高速','G80广昆高速','G12珲乌高速','G16丹锡高速','G36宁洛高速','G78汕昆高速')
          | then t2.roadName else 'Others' end) as highway_code
          |from hb_province_data t1 left join highway_data t2 on regexp_extract(t1.Latitude,'([0-9]*.[0-9][0-9][0-9][0-9])') = t2.lat and regexp_extract(t1.Longitude,'([0-9]*.[0-9][0-9][0-9][0-9])') = t2.lgt
          |""".stripMargin)
        .withColumn("Landscape",getLandscape($"Altitude",$"Barometric_Pressure"))
        .withColumn("Climate_Zone",getClimateZone($"province",$"city",$"month"))
        .show()
      //        .createOrReplaceTempView("clean_data")

    }

    // TODO 关联master获取engine_name字段
    //    sql(
    //      """
    //        |select t1.*,t2.engine_name
    //        |from clean_data t1 join primary.esnmaster t2 on t1.Engine_Serial_Number = t2.esn
    //        |""".stripMargin)
    //        .createOrReplaceTempView("res_data")

    // TODO 写入hive表
//    sql(
//      """
//        |insert into table
//        |""".stripMargin)

  }

  def getLandscape = udf {
    //, Barometric_Pressure: Double
    (Altitude:String,Barometric_Pressure:String) =>
      var value = ""
      var altitude= -1.0
      if (Altitude != null&& Altitude !=""){
        altitude =  Altitude.toInt
        //&& Barometric_Pressure !=""
      }else if (Barometric_Pressure != null&&Barometric_Pressure.equals("null")&& Altitude.equals("")||Altitude == null){
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
    //, Barometric_Pressure: Double
    (province:String,city:String,month:String) =>
      var value= "Others"
      if (region.contains(province)){
        if (province_Xinjiang.equals(province)&&city_Tulufan.equals(city)&& month_dry.contains(month)){value="Hot_Dry_Area"}
        else if (province_Hainan.equals(province)&&month_humid.contains(month)){value="Hot_Humid_Area"}
        else if (province_HeiQingXi.contains(province)&&month_cold.contains(month)){value="Cold_Region"}
        else if (province_InnerMongolia.equals(province)&&city_InnerMongolia.contains(city)&&month_cold.contains(month)){value="Cold_Region"}
        else if (province_Gansu.equals(province)&&city_Gansu.contains(city)&&month_cold.contains(month)){value="Cold_Region"}
        else if (province_Sichuan.equals(province)&&city_Sichuan.contains(city)&&month_cold.contains(month)){value="Cold_Region"}
        else if (province_Jilin.equals(province)&&city_Jilin.contains(city)&&month_cold.contains(month)){value="Cold_Region"}
        else if (province_Xinjiang.equals(province)&&city_Xinjiang.contains(city)&&month_cold.contains(month)){value="Cold_Region"}
      }
      value
  }

}
