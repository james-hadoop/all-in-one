package com.james.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.james.temp.TableLineageInfo;

public class SqlLineageUtil {
	/**
	 * fieldAliasMap中递归查找原始字段的名称
	 * 
	 * @param queryField
	 * @param fieldAliasMap
	 * @return
	 */
	public static String findSrcField(String queryField, Map<String, String> fieldAliasMap) {
		if (null == queryField || null == fieldAliasMap || 0 == fieldAliasMap.size()) {
			return null;
		}

		String srcField = fieldAliasMap.get(queryField);
		if (null == srcField || queryField.equals(srcField)) {
			return queryField;

		} else {
			return findSrcField(srcField, fieldAliasMap);
		}
	}
	
	/**
	 * tableAliasLineageMap中查找原始表的名称
	 * 
	 * @param tableAliasName
	 * @param tableAliasLineageMap
	 * @return
	 */
	public static Set<String> findSrcTable(String tableAliasName,Map<String,TableLineageInfo> tableAliasLineageMap) {
		if(null==tableAliasName||null==tableAliasLineageMap||0==tableAliasLineageMap.size()) {
			return null;
		}
		
		return tableAliasLineageMap.get(tableAliasName).getTableAliasReferMap().keySet();
	}
	
	public static Set<String> findSrcTable(String tableAliasName,Map<String,TableLineageInfo> tableAliasLineageMap,Stack<String> tokTableNameStack ) {
		if(null==tableAliasName||null==tableAliasLineageMap||0==tableAliasLineageMap.size()) {
			return null;
		}
		
		return tableAliasLineageMap.get(tableAliasName).getTableAliasReferMap().keySet();
	}
}
