package com.wuyuhang.tools.filecreate

import com.wuyuhang.tools.FileWriteTool

object HqlFile extends FileWriteTool{
	def hqlFileMaker(path: Path, fileName: String): Unit = {
		if (fileName.toUpperCase.contains("ETL_")) {
			val path_cream = path.split("/").dropRight(1).mkString("/") + "/etl"
			scalaWriteOverWrite(path_cream)(fileName + ".hql")("")
		}
	}
}
