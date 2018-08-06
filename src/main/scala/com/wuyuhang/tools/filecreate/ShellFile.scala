package com.wuyuhang.tools.filecreate

import java.text.SimpleDateFormat
import java.util.Date

import com.wuyuhang.tools.FileWriteTool

object ShellFile extends FileWriteTool {
	/**
		* shell脚本选择器
		* 根据文件名称筛选shell脚本执行其中包含，全量脚本，增量脚本，生成事件脚本
		*
		* @param path     文件路径
		* @param fileName 文件名称
		* @param db_info  数据库链接信息
		*/
	
	def shellFileMaker(path: Path, fileName: String, db_info: Map[String, List[String]], searchStr: String, projectName: String) {
		//解析数据库配置信息
		var url_source = ""
		var port_source = ""
		var user_source = ""
		var password_source = ""
		var database_source = ""
		var table_source: List[String] = Nil
		
		
		//目标信息（全量）
		var database_target = ""
		var table_target = ""
		
		//目标信息（增量）
		var database_target_cream = ""
		var table_target_cream = ""
		
		//增量抽取标记，增量抽取语句
		var incream_flag = ""
		var querry_sql = ""
		
		//复杂sql
		
		var sql_complex_flag = ""
		var a_querry_sql = ""
		var a_where_sql = ""
		var a_and_sql = ""
		var union_sql = ""
		var b_querry_sql = ""
		var b_where_sql = ""
		var b_and_sql = ""
		
		//数据源信息
		if (searchStr != "") {
			url_source = db_info("url_source" + searchStr).mkString
			port_source = db_info("port_source" + searchStr).mkString
			user_source = db_info("user_source" + searchStr).mkString
			password_source = db_info("password_source" + searchStr).mkString
			database_source = db_info("database_source" + searchStr).mkString
			table_source = db_info("table_source" + searchStr)
			
			incream_flag = db_info("incream_flag" + searchStr).mkString
			
			
			querry_sql = db_info("querry_sql" + searchStr).mkString
			
			
			//复杂sql
			sql_complex_flag = db_info("sql_complex_flag" + searchStr).mkString
			a_querry_sql = db_info("a_querry_sql" + searchStr).mkString
			a_where_sql = db_info("a_where_sql" + searchStr).mkString
			a_and_sql = db_info("a_and_sql" + searchStr).mkString
			union_sql = db_info("union_sql" + searchStr).mkString
			b_querry_sql = db_info("b_querry_sql" + searchStr).mkString
			b_where_sql = db_info("b_where_sql" + searchStr).mkString
			b_and_sql = db_info("b_and_sql" + searchStr).mkString
			
			
			
			
			//目标信息（全量）
			
			scalaWriteOverWrite(path.dropRight(5) + "password")("password.file")(password_source)
			
			password_source = s"--password-file hdfs://turinghdfs:8020/user/hdfs/password/$projectName/password.file"
			
			database_target = db_info("database_target" + searchStr).mkString
			table_target = db_info("table_target" + searchStr).mkString
			
			//目标信息（增量）
			database_target_cream = db_info("database_target_cream" + searchStr).mkString
			table_target_cream = db_info("table_target_cream" + searchStr).mkString
		}
		//根据文件名信息筛选要指定执行的脚本
		if (fileName.toUpperCase.contains("_WF")) {
			//执行事件生成脚本
			eventFile(path, fileName)
		} else if (fileName.toUpperCase.contains("SQOOP_")) {
			//执行数据加载脚本
			if (fileName.toUpperCase.contains("_S_")) {
				//执行增量加载脚本
				for (table_src <- table_source) {
					
					//以下用正则表达式区分一张表的名称包含在另一张表中的情况，例如 t_area t_area_101，如果用t_area去匹配，会匹配出两张表
					import scala.util.matching.Regex
					val tab = "_" + table_src + "_" + "[0-9][0-9]" + "$"
					val pattern = new Regex(tab)
					
					val fntc = fileName.toLowerCase
					
					val matched = (pattern findAllIn fntc).mkString
					
					if (matched.isEmpty) {
					} else {
						creamFile(path, fileName, url_source, port_source, user_source
							, password_source, database_source, table_src, database_target_cream, table_target_cream)
					}
				}
			} else {
				//执行全量加载脚本
				for (table_src <- table_source) {
					//以下用正则表达式区分一张表的名称包含在另一张表中的情况，例如 t_area t_area_101，如果用t_area去匹配，会匹配出两张表
					import scala.util.matching.Regex
					val tab = "_" + table_src + "_" + "[0-9][0-9][0-9][0-9]" + "$"
					val pattern = new Regex(tab)
					
					val fntc = fileName.toLowerCase
					
					val matched = (pattern findAllIn fntc).mkString
					if (matched.isEmpty) {
					} else {
						
						amountFile(path, fileName, url_source, port_source, user_source
							, password_source, database_source, table_src, database_target, table_target, incream_flag, querry_sql,
							sql_complex_flag, a_querry_sql, a_where_sql, a_and_sql, union_sql, b_querry_sql, b_where_sql, b_and_sql)
					}
				}
			}
		}
	}
	
	/**
		* 加载全量数据脚本
		*
		* @param path            文件路径
		* @param fileName        文件名称
		* @param url_source      数据源链接urk
		* @param port_source     数据源端口
		* @param user_source     数据源用户名称
		* @param password_source 数据源密码信息
		* @param database_source 数据源数据库
		* @param table_src       数据源表
		* @param database_target 目标数据库
		* @param table_target    目标数据表
		*/
	def amountFile(path: Path, fileName: String, url_source: String, port_source: String, user_source: String, password_source: String
								 , database_source: String, table_src: String, database_target: String, table_target: String, incream_flag: String, querry_sql: String,
								 sql_complex_flag: String, a_querry_sql: String, a_where_sql: String, a_and_sql: String, union_sql: String,
								 b_querry_sql: String, b_where_sql: String, b_and_sql: String) {
		
		import scala.util.matching.Regex
		
		//val pattens = "tb_student_points_record_[0-9]"
		
		
		//匹配城市id
		val num = "[0-9]+"
		val pattern_num = new Regex(num)
		
		val fntc = fileName.toLowerCase
		
		val match_num = (pattern_num findAllIn fntc).mkString
		
		var partition_para = ""
		if (match_num.isEmpty) {
			partition_para = ""
		} else {
			//此处暂时不需要分区参数
			partition_para = ""
			//partition_para = s"--hive-partition-key city_id --hive-partition-value \'$match_city\' "
		}
		
		if (match_num.isEmpty) {
		
		} else {
			
			if (incream_flag != "1") {
				
				/**
					* 如果不是增量抽取，那么使用初始化脚本运行
					*/
				
				//如果像tb_student_points_record_010这样的表的话，将目标表 同为合并为表tb_student_points_record
				var overwrite_para = ""
				//if (match_num.equals("01")) {
				overwrite_para = "--hive-overwrite"
				//}
				partition_para = s" --hive-partition-key database_id --hive-partition-value ${match_num}"
				
				var target_dir = s"--target-dir ${table_target}_${match_num} "
				val command_lines: String =
					s"""#!/bin/bash
						 |export JAVA_HOME="/usr/local/jdk1.7.0"
						 |#${table_target.toUpperCase}
						 |#import ${table_target}
						 |hdfs dfs -test -e /user/hdfs/${table_target}_${match_num}
						 |if [ $$? -eq 0 ] ;then
						 |	echo 'Directory already exist.Delete Directory'
						 | 	hdfs dfs -rm -r /user/hdfs/${table_target}_${match_num}
						 |else
						 |	echo 'Directory is not exist.Can be started.'
						 |fi
						 |sqoop import --connect $url_source --username $user_source $password_source --table $table_src --hive-import --hive-database $database_target --hive-table ${table_target} $overwrite_para $partition_para $target_dir --hive-drop-import-delims -m 1
						 |""".stripMargin
				
				scalaWriteOverWrite(path)(fileName + ".sh")(command_lines)
			} else {
				
				if (sql_complex_flag == "1") {
					/**
						* 复杂抽取语句
						*/
					//如果像tb_student_points_record_010这样的表的话，将目标表 同为合并为表tb_student_points_record
					var overwrite_para = ""
					//if (match_num.equals("01")) {
					overwrite_para = "--hive-overwrite"
					//}
					partition_para = s" --hive-partition-key database_id --hive-partition-value ${match_num}"
					var target_dir = s"--target-dir ${table_target}_${match_num} "
					val command_lines: String =
						s"""#!/bin/bash
							 |export JAVA_HOME="/usr/local/jdk1.7.0"
							 |#${table_target.toUpperCase}
							 |#import ${table_target}
							 |s_date=$$(date -d "-1 days" +%Y-%m-%d)
							 |if [ $$# -eq 1 ]; then
							 |	s_date = $$1
							 |fi
							 |hdfs dfs -test -e /user/hdfs/${table_target}_${match_num}
							 |if [ $$? -eq 0 ] ;then
							 |	echo 'Directory already exist.Delete Directory'
							 | 	hdfs dfs -rm -r /user/hdfs/${table_target}_${match_num}
							 |else
							 |	echo 'Directory is not exist.Can be started.'
							 |fi
							 |sqoop import --connect $url_source --username $user_source $password_source  --hive-import --hive-database $database_target --hive-table ${table_target} $overwrite_para $partition_para $target_dir --hive-drop-import-delims -m 1 --null-string '\\\\N' --null-non-string '\\\\N' --query \\
							 |${a_querry_sql.replace("@", ",").dropRight(1)} ${a_where_sql.dropRight(1).replace("\"", "")}  ${a_and_sql.dropRight(1).replace("\"", "").replace("@", ",").replace("<", "=")} \'$${s_date}' and \\$$CONDITIONS \\
							 |${union_sql.dropRight(1).replace("\"", "")} \\
							 |${b_querry_sql.replace("@", ",").dropRight(1).replace("\"", "")} ${b_where_sql.dropRight(1).replace("\"", "")}  ${b_and_sql.dropRight(1).replace("\"", "").replace("@", ",").replace("<", "=")} \'$${s_date}' and \\$$CONDITIONS"
							 |""".stripMargin
					
					scalaWriteOverWrite(path)(fileName + ".sh")(command_lines)
					
					
				} else {
					/**
						* 如果是增量抽取，那么使用查询脚本执行查询
						*/
					//如果像tb_student_points_record_010这样的表的话，将目标表 同为合并为表tb_student_points_record
					var overwrite_para = ""
					//if (match_num.equals("01")) {
					overwrite_para = "--hive-overwrite"
					//}
					partition_para = s" --hive-partition-key database_id --hive-partition-value ${match_num}"
					var target_dir = s"--target-dir ${table_target}_${match_num} "
					val command_lines: String =
						s"""#!/bin/bash
							 |export JAVA_HOME="/usr/local/jdk1.7.0"
							 |#${table_target.toUpperCase}
							 |#import ${table_target}
							 |s_date=$$(date -d "-1 days" +%Y-%m-%d)
							 |if [ $$# -eq 1 ]; then
							 |	s_date = $$1
							 |fi
							 |hdfs dfs -test -e /user/hdfs/${table_target}_${match_num}
							 |if [ $$? -eq 0 ] ;then
							 |	echo 'Directory already exist.Delete Directory'
							 | 	hdfs dfs -rm -r /user/hdfs/${table_target}_${match_num}
							 |else
							 |	echo 'Directory is not exist.Can be started.'
							 |fi
							 |sqoop import --connect $url_source --username $user_source $password_source  --hive-import --hive-database $database_target --hive-table ${table_target} $overwrite_para $partition_para $target_dir --hive-drop-import-delims -m 1 --query ${querry_sql.dropRight(1).replace("@", ",").replace("<", "=")} \'$${s_date}' and \\$$CONDITIONS "
							 |""".stripMargin
					
					scalaWriteOverWrite(path)(fileName + ".sh")(command_lines)
				}
			}
			
			
		}
		
	}
	
	/**
		* 增量数据加载脚本
		*
		* @param path            文件路径
		* @param fileName        文件名称
		* @param url_source      数据源url链接
		* @param port_source     数据源端口
		* @param user_source     数据源用户
		* @param password_source 数据源密码
		* @param database_source 数据源数据库
		* @param table_src       数据源表
		* @param database_target 目标数据库
		* @param table_target    目标数据表
		*/
	def creamFile(path: Path, fileName: String, url_source: String, port_source: String, user_source: String, password_source: String
								, database_source: String, table_src: String, database_target: String, table_target: String) {
		
		import scala.util.matching.Regex
		
		//val pattens = "tb_student_points_record_[0-9]"
		
		val tab_re = "tb_student_points_record_[0-9]"
		val tab_red = "tb_student_points_record_detail_[0-9]"
		val pattern_re = new Regex(tab_re)
		val pattern_red = new Regex(tab_red)
		
		//匹配城市
		val city_re = "[0-9]+"
		val pattern_city = new Regex(city_re)
		
		
		val fntc = fileName.toLowerCase
		
		
		val match_re = (pattern_re findAllIn fntc).mkString.dropRight(2)
		val match_red = (pattern_red findAllIn fntc).mkString.dropRight(2)
		val match_city = (pattern_city findAllIn fntc).mkString
		
		if (match_re.isEmpty) {
			if (match_red.isEmpty) {
				val command_lines: String =
					s"""#!/bin/bash
						 |export JAVA_HOME="/usr/local/jdk1.7.0"
						 |#${table_target.toUpperCase + table_src.toUpperCase}
						 |#import ${table_target + table_src}
						 |s_date=$$(date -d \'-7 days\' +%Y-%m-%d)
						 |if [ $$# -eq 1 ]; then
						 |	s_date=$$1
						 |fi
						 |sqoop import --connect $url_source --username $user_source --password $password_source --hive-import --hive-database $database_target --hive-table ${table_target + table_src} --target-dir ${table_target + table_src} --hive-overwrite --query \"select * from ${table_src} where substr(spr_create_date,1,10)>=\'$${s_date}\' and \\$$CONDITIONS\" --hive-drop-import-delims -m 1
						 |""".stripMargin
				val path_cream = path.split("/").dropRight(1).mkString("/") + "/incream"
				scalaWriteOverWrite(path_cream)(fileName + ".sh")(command_lines)
			} else {
				var overwrite_para = ""
				if (match_city.equals("01")) {
					overwrite_para = "--hive-overwrite"
				}
				
				val command_lines: String =
					s"""#!/bin/bash
						 |export JAVA_HOME="/usr/local/jdk1.7.0"
						 |#${table_target.toUpperCase + match_red.toUpperCase}
						 |#import ${table_target + match_red}
						 |s_date=$$(date -d \'-7 days\' +%Y-%m-%d)
						 |if [ $$# -eq 1 ]; then
						 |	s_date=$$1
						 |fi
						 |
						 |sqoop import --connect $url_source --username $user_source --password $password_source --hive-import --hive-database $database_target --hive-table ${table_target + match_red} --target-dir ${table_target + match_red} $overwrite_para --query \"select * from $table_src where substr(spr_create_date,1,10)>=\'$${s_date}\' and \\$$CONDITIONS\" --hive-drop-import-delims -m 1
						 |""".stripMargin
				val path_cream = path.split("/").dropRight(1).mkString("/") + "/incream"
				scalaWriteOverWrite(path_cream)(fileName + ".sh")(command_lines)
			}
		} else {
			var overwrite_para = ""
			if (match_city.equals("01")) {
				overwrite_para = "--hive-overwrite"
			}
			val command_lines: String =
				s"""#!/bin/bash
					 |export JAVA_HOME="/usr/local/jdk1.7.0"
					 |#${table_target.toUpperCase + match_re.toUpperCase}
					 |#import ${table_target + match_re}
					 |s_date=$$(date -d \'-7 days\' +%Y-%m-%d)
					 |if [ $$# -eq 1 ]; then
					 |	s_date=$$1
					 |fi
					 |sqoop import --connect $url_source --username $user_source --password $password_source --hive-import --hive-database $database_target --hive-table ${table_target + match_re} --target-dir ${table_target + match_re} $overwrite_para --query \"select * from $table_src where substr(spr_create_date,1,10)>=\'$${s_date}\' and \\$$CONDITIONS\" --hive-drop-import-delims -m 1
					 |""".stripMargin
			val path_cream = path.split("/").dropRight(1).mkString("/") + "/incream"
			scalaWriteOverWrite(path_cream)(fileName + ".sh")(command_lines)
		}
		
	}
	
	/**
		* 创建抽取调度动作完成后的事件生成脚本
		*
		* @param path     文件路径
		* @param fileName 文件名称
		*/
	def eventFile(path: Path, fileName: String) {
		val schemeName = fileName.dropRight(2)
		val now: Date = new Date()
		val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
		val today = dateFormat.format(now)
		val command_lines: String =
			s"""#!/bin/bash
				 |hadoop fs -touchz /tmp/azkaban_event/$schemeName$$(date +"%Y%m%d").event
				 |""".stripMargin
		val path_event = path.split("/").dropRight(1).mkString("/") + "/gen_event"
		scalaWriteOverWrite(path_event)(fileName + ".sh")(command_lines)
		
		
	}
}
