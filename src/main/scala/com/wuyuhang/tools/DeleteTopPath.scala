package com.wuyuhang.tools

trait DeleteTopPath {
	/**
		*
		* @param mapdep  临接表映射Map
		* @param map_key Map的key值
		* @param mp      空映射，承接删除顶点和顶点相关路径的DAG
		* @return 删除顶点和顶点相关路径后的DAG
		*/
	def deletePath(mapdep: Map[String, List[String]], map_key: String, mp: Map[String, List[String]]): Map[String, List[String]] = {
		var mmp = mp
		for ((m_key, m_value) <- mapdep) {
			var vf: List[String] = m_value.filter(_ != map_key)
			mmp += (m_key -> vf)
		}
		mmp
	}
}
