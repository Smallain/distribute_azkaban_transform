package com.wuyuhang.tools

import com.wuyuhang.DAG

trait TopoSort {
	this: DAG.type =>
	
	/**
		*
		* @param mapdepIn：临接表映射Map
		* @return 拓扑排序列表
		*/
	
	def topology(mapdepIn: Map[String, List[String]]): List[String] = {
		var mapdep = mapdepIn
		var mmap = Map[String, List[String]]()
		//判断传入参数文件是否为空
		if (mapdep.isEmpty) {
			println("输入配置文件解析为空！")
		} else {
			//查找入度为0度的顶点
			val map_key = getDagTop(mapdep)
			//根据入度为0的key，value删除顶点
			topolist = topolist :+ map_key
			mapdep -= map_key
			//删除与顶点相连的路径
			var mmp = deletePath(mapdep, map_key, mmap)
			//如果此时有向无环图不为空，尾递归继续查询（拓扑排序）
			if (mapdep.isEmpty) {
				println("拓扑排序完成!")
			} else {
				topology(mmp)
			}
		}
		topolist
	}
}
