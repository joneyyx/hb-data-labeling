package com.cummins.dataLabeling.common.config

import com.typesafe.config.ConfigFactory

object AppConfig extends Serializable {



    val objConfig = ConfigFactory.load("application.properties")

    val sqlDriver = objConfig.getString("jdbc.driver")
    val sqlUrl = objConfig.getString("jdbc.url")
    val sqlUser = objConfig.getString("jdbc.username")
    val sqlPW = objConfig.getString("jdbc.password")

    val destSqlUrl = objConfig.getString("jdbc.dst.url")
    val destSqlUser = objConfig.getString("jdbc.dst.username")
    val destSqlPW = objConfig.getString("jdbc.dst.password")

    val blobStorageAccount = objConfig.getString("blob.storage.account")

}
