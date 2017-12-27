package com.wuyuhang

import com.wuyuhang.tools.FileWriteTool
import com.wuyuhang.tools.filecreate.HqlFile
import com.wuyuhang.tools.filecreate.JobFile
import com.wuyuhang.tools.filecreate.ShellFile

object MainApp extends App with FileWriteTool {
	
	val mp = DAG.analysisFile("./src/main/resources/SQOOP_TEMP.txt")
	
	val listSort = DAG.topology(mp).filter(_ != "")
	
	
	/**
		* 生成Azkaban调度程序文件
		*/
	
	var command_lines = ""
	for (i <- listSort) {
		val deps = mp(i).mkString(",")
		JobFile.jobFileMaker("./src/main/resources/SQOOP_TEMP/azkaban", i, deps)
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
			val tab = "_[0-9][0-9]$"
			val pattern = new Regex(tab)
			val fntc = i.toLowerCase
			val matched_num = (pattern findAllIn fntc).mkString
			println(i)
			val db_info = DAG.analysisDBFile("./src/main/resources/SQOOP_TEMP_DB.txt", matched_num).filter(k => k._2 != Nil)
			ShellFile.shellFileMaker("./src/main/resources/SQOOP_TEMP/sqoop", i, db_info, matched_num)
		}
	}
	
	/**
		* 生成hql脚本调度程序文件
		*/
	
	for (i <- listSort) {
		HqlFile.hqlFileMaker("./src/main/resources/SQOOP_TEMP/etl", i)
	}
}
