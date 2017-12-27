package com.wuyuhang.tools

trait GetDAGTop {
	/**
		*
		* @param map ：DAG邻接表映射Map
		* @return ：Map的key值
		*/
	def getDagTop(map: Map[String, List[String]]): String = {
		var map_value = List()
		var map_key = ""
		for ((mk, mv) <- map) {
			if (mv == List()) {
				map_key = mk
			}
		}
		map_key
	}
}
