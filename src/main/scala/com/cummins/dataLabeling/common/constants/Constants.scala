package com.cummins.dataLabeling.common.constants

import com.typesafe.config.ConfigFactory

object Constants {

  val Primary_Engine_Build: String = "qa_dev_primary.rel_engine_detail"
//  val Primary_Engine_Build: String = "BUILD_ENGINE"
  val Raw_Engine_Base_Detail: String = "ENGINE_BASE_DETAIL"
  val Primary_EsnMaster: String = "pri_tel.esn_master_detail"
  val primary_trim_data: String = "qa_dev_primary.tel_csu_trim"

  //sqlServer
  val Table_EsnMaster: String = "ESN_MASTER_DETAIL"

//  val Primary_Engine_Build: String = "BUILD_ENGINE"
//  val Raw_Engine_Base_Detail: String = "ENGINE_BASE_DETAIL"
//  val Primary_EsnMaster: String = "PRI_ESNMASTER"

  //labeling hive表
  val objConfig = ConfigFactory.load("application.properties")
  val gps_data = objConfig.getString("gps_data")
  val highway_data = objConfig.getString("highway_data")
  val engine_master_detail = objConfig.getString("engine_master_detail")
  val hblabeling_tmp = objConfig.getString("hblabeling_tmp")
  //hdfs path, for get warehouse
  val hdfs_path = objConfig.getString("hdfs_path")

  //climate
  val region : List[String] = List("新疆维吾尔自治区","海南省","黑龙江省","青海省","西藏自治区","内蒙古自治区","甘肃省","四川省","吉林省")

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

}
