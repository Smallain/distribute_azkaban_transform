package com.wuyuhang

import com.wuyuhang.tools.DeleteTopPath
import com.wuyuhang.tools.GetDAGTop
import com.wuyuhang.tools.LoadAdjacencyList
import com.wuyuhang.tools.TopoSort

object DAG extends GetDAGTop with DeleteTopPath with TopoSort with LoadAdjacencyList {
	
	var topolist: List[String] = List()
	
}
