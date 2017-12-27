package com.wuyuhang.tools.filecreate

import com.wuyuhang.tools.FileWriteTool

object JobFile extends FileWriteTool {
	
	/**
		* path:							输出文件路径
		* fileName：					输出文件名称
		* dependenciesFile：工程拓扑临接关系
		*/
	
	
	def jobFileMaker(path: Path, fileName: String, dependenciesFile: String) {
		if (fileName.toUpperCase.contains("_START")) {
			startFile(path, fileName, dependenciesFile)
		} else if (fileName.toUpperCase.contains("ETL_")) {
			hqlFileMaker(path, fileName, dependenciesFile)
		} else if (fileName.toUpperCase.contains("_END")) {
			endFile(path, fileName, dependenciesFile)
		} else if (fileName.toUpperCase.contains("_WF")) {
			wfFile(path, fileName, dependenciesFile)
		} else if (fileName.toUpperCase.contains("SQOOP_")) {
			shellFileMaker(path, fileName, dependenciesFile)
		}
	}
	
	def startFile(path: Path, fileName: String, dependenciesFile: String): Unit = {
		val command_lines: String =
			s"""#$fileName
				 |type=command
				 |command=echo "sync data $fileName start"
				 |user.to.proxy=hdfs""".stripMargin
		
		scalaWriteOverWrite(path)(fileName + ".job")(command_lines)
	}
	
	
	def shellFileMaker(path: Path, fileName: String, dependenciesFile: String) {
		if (fileName.toUpperCase.contains("_S_")) {
			val command_lines: String =
				s"""#$fileName
					 |type=command
					 |dependencies=$dependenciesFile
					 |command=/bin/bash ../incream/$fileName.sh"
					 |user.to.proxy=hdfs""".stripMargin
			
			scalaWriteOverWrite(path)(fileName + ".job")(command_lines)
		} else {
			val command_lines: String =
				s"""#$fileName
					 |type=command
					 |dependencies=$dependenciesFile
					 |command=/bin/bash ../sqoop/$fileName.sh"
					 |user.to.proxy=hdfs""".stripMargin
			
			scalaWriteOverWrite(path)(fileName + ".job")(command_lines)
		}
		
	}
	
	def hqlFileMaker(path: Path, fileName: String, dependenciesFile: String) {
		val command_lines: String =
			s"""#$fileName
				 |type=command
				 |dependencies=$dependenciesFile
				 |command=hive -f ../etl/$fileName.hql
				 |user.to.proxy=hdfs""".stripMargin
		
		scalaWriteOverWrite(path)(fileName + ".job")(command_lines)
	}
	
	def endFile(path: Path, fileName: String, dependenciesFile: String): Unit = {
		val command_lines: String =
			s"""#$fileName
				 |type=command
				 |dependencies=$dependenciesFile
				 |command=echo "sync data $fileName end"
				 |user.to.proxy=hdfs""".stripMargin
		
		scalaWriteOverWrite(path)(fileName + ".job")(command_lines)
	}
	
	def wfFile(path: Path, fileName: String, dependenciesFile: String): Unit = {
		val command_lines: String =
			s"""#$fileName
				 |type=command
				 |dependencies=$dependenciesFile
				 |command=/bin/bash ../gen_event/$fileName.sh
				 |user.to.proxy=hdfs""".stripMargin
		
		scalaWriteOverWrite(path)(fileName + ".job")(command_lines)
	}
}
