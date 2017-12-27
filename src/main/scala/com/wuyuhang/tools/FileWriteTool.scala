package com.wuyuhang.tools

import better.files.File
import better.files._
import better.files.Dsl._

trait FileWriteTool {
	type Path = String
	
	/**
		* scala io 文件写入
		* appendLine:文本末尾追加行内容
		*
		* @param path     文件夹路径
		* @param fileName 文件名称
		* @param messages 文件内容
		*/
	def scalaWriteAppendLine(path: Path)(fileName: String)(messages: String): Unit = {
		val fileDir = File(path)
		if (fileDir.isDirectory) {
			//文件路径存在
			val file = fileDir / fileName
			file.appendLine(messages)
		} else {
			//文件路径不存在
			mkdirs(fileDir)
			val file = fileDir / fileName
			file.appendLine(messages)
		}
	}
	
	/**
		* scala io 文件写入
		* append:文本末尾追加内容
		*
		* @param path     文件夹路径
		* @param fileName 文件名称
		* @param messages 文件内容
		*/
	def scalaWriteAppend(path: Path)(fileName: String)(messages: String): Unit = {
		val fileDir = File(path)
		if (fileDir.isDirectory) {
			//文件路径存在
			val file = fileDir / fileName
			file.append(messages)
		} else {
			//文件路径不存在
			mkdirs(fileDir)
			val file = fileDir / fileName
			file.append(messages)
		}
	}
	
	/**
		* scala io 文件写入
		* overwrite:覆盖重写文件
		*
		* @param path     文件夹路径
		* @param fileName 文件名称
		* @param messages 文件内容
		*/
	def scalaWriteOverWrite(path: Path)(fileName: String)(messages: String): Unit = {
		val fileDir = File(path)
		if (fileDir.isDirectory) {
			//文件路径存在
			val file = fileDir / fileName
			file.overwrite(messages)
		} else {
			//文件路径不存在
			mkdirs(fileDir)
			val file = fileDir / fileName
			file.overwrite(messages)
		}
	}
}
