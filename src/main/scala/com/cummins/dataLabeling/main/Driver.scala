package com.cummins.dataLabeling.main

import com.cummins.dataLabeling.primary.telematics.{EsnMasterJob, TelHBLabledPrimaryJob}
import com.cummins.dataLabeling.util.{DateUtil, Util}
import org.apache.log4j.Logger

object Driver {

	val logger = Logger.getLogger(this.getClass)

	implicit val spark = Util.getSpark
	val sc = spark.sparkContext
	val sqlContext = spark.sqlContext
	sc.setLogLevel("INFO")

	def main(args: Array[String]): Unit = {

		val start = System.currentTimeMillis()
		logger.info(s"Starting Job.")

		// 处理业务逻辑

		//    EsnMasterJob.processMasterData(spark,sc,args(0))

		//执行labeling表-不跑CMCC
		val beijingYesterday = DateUtil.getBeijingYesterday
		    val array = Array("Zhike", "Cty", "G7", "DF", "LG", "JAC")
		if (args == null || args.length == 0) {
			for (elem <- array) {
				TelHBLabledPrimaryJob.exec(spark, elem, beijingYesterday)
				logger.info(elem + " : succeed")
			}
		} else {
			for (elem <- array) {
				try {
					TelHBLabledPrimaryJob.exec(spark, elem, args(0))
					logger.info(elem + ">>>>>>>: succeed, on date:" + args(0))
				} catch {
					case e: Exception => {
						println(s">>>>>>>error occurred")
						logger.info(elem + ">>>>>>>: failed, the specific date is " + args(0) + "and the TSP is :" + elem)
					}
				}
			}


			//for dev running tmp
			//    for (elem <- array) {
			//      TelHBLabledPrimaryJob.exec(spark,elem,args(0))
			//      logger.info(elem+" : succeed")
			//    }
			logger.info(s"Finished Job in ${(System.currentTimeMillis() - start) / 1000} seconds.")

			spark.close()
		}

	}
}
