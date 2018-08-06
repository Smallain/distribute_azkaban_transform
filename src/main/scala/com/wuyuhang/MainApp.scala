package com.wuyuhang

import com.wuyuhang.tools.FileWriteTool
import com.wuyuhang.tools.filecreate.HqlFile
import com.wuyuhang.tools.filecreate.JobFile
import com.wuyuhang.tools.filecreate.ShellFile

object MainApp extends App with FileWriteTool {
	
	
	val projectName = "SQOOP_ODS_LEGO_ORDER_TB_ORDER_XESCLASS_GROUP"
	
	
	val mp = DAG.analysisFile("./src/main/resources/SQOOP_ODS_LEGO_ORDER_TB_ORDER_XESCLASS_GROUP.txt")
	
	val listSort = DAG.topology(mp).filter(_ != "")
	
	
	/**
		* 生成Azkaban调度程序文件
		*/
	
	var command_lines = ""
	for (i <- listSort) {
		val deps = mp(i).mkString(",")
		JobFile.jobFileMaker("./src/main/resources/SQOOP_ODS_LEGO_ORDER_TB_ORDER_XESCLASS_GROUP/azkaban", i, deps)
		var tmp_s = s"输出队列是：$i								依赖是：$deps\n"
		command_lines += tmp_s
		scalaWriteOverWrite("./src/main/resources/")("dag_squeue.txt")(command_lines)
	}
	
	/**
		* 生成shell脚本调度程序文件
		*/
	
	for (i <- listSort) {
		if (!(i.contains("_START") || i.contains("_END"))) {
			import scala.util.matching.Regex
			val tab = "_[0-9][0-9][0-9][0-9]$"
			val pattern = new Regex(tab)
			val fntc = i.toLowerCase
			val matched_num = (pattern findAllIn fntc).mkString
			println(i)
			val db_info = DAG.analysisDBFile("./src/main/resources/SQOOP_ODS_LEGO_ORDER_TB_ORDER_XESCLASS_GROUP_DB.txt", matched_num).filter(k => k._2 != Nil)
			//println(db_info)
			ShellFile.shellFileMaker("./src/main/resources/SQOOP_ODS_LEGO_ORDER_TB_ORDER_XESCLASS_GROUP/sqoop", i, db_info, matched_num,projectName)
		}
	}
	
	/**
		* 生成hql脚本调度程序文件
		*/
	
	for (i <- listSort) {
		HqlFile.hqlFileMaker("./src/main/resources/SQOOP_ODS_LEGO_ORDER_TB_ORDER_XESCLASS_GROUP/etl", i)
	}
}
