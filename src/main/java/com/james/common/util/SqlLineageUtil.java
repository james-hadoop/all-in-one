package com.james.common.util;

import java.util.HashSet;
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
	public static Set<String> findSrcTable(String tableAliasName, Map<String, TableLineageInfo> tableAliasLineageMap) {
		if (null == tableAliasName || null == tableAliasLineageMap || 0 == tableAliasLineageMap.size()) {
			return null;
		}

		return tableAliasLineageMap.get(tableAliasName).getTableAliasReferMap().keySet();
	}

	public static Set<String> findSrcTable(String tableAliasName, Map<String, TableLineageInfo> tableAliasLineageMap,
			Stack<String> tokTableNameStack) {
		if (null == tableAliasName || null == tableAliasLineageMap || 0 == tableAliasLineageMap.size()) {
			return null;
		}

		return tableAliasLineageMap.get(tableAliasName).getTableAliasReferMap().keySet();
	}

	/*
	 * vv -> c.vv
	 */
	public static Set<String> addAliasName(String field, Map<String, String> topLevelTableAliasMap) {
		if (null == field) {
			return null;
		}

		Set<String> set = new HashSet<String>();

		if (field.contains(".")) {
			set.add(field);
		} else {
			Set<String> s = topLevelTableAliasMap.keySet();
			for (String k : s) {
				set.add(k + "." + field);
			}
		}

		return set;
	}

	/*
	 * c.puin -> a.puin & b.puin
	 */
	public static Set<String> addReferTableName(String field, Map<String, TableLineageInfo> tableAliasLineageMap) {
		if (null == field || !field.contains(".")) {
			return null;
		}

		TableLineageInfo info = tableAliasLineageMap.get(field.substring(0, field.indexOf(".")));
		if (null == info) {
			return null;
		}

		Set<String> set = new HashSet<String>();

		if (info.getTableAliasReferMap().size() > 1) {
			for (String k : info.getTableAliasReferMap().keySet()) {
				set.add(k + "." + field.substring(field.indexOf(".") + 1));
			}
		} else {
			set.add(field);
		}

		return set;
	}

	/*
	 * b.history_vv -> op_cnt
	 * 
	 * b.history_vv -> b.op_cnt
	 */
	public static Set<String> addAliasName2(String field, Map<String, String> fieldAliasMap2) {
		if (null == field || !field.contains(".")) {
			return null;
		}

		String mappingField = fieldAliasMap2.get(field);
		if (null == mappingField) {
			return null;
		}

		Set<String> set = new HashSet<String>();
		String aliasName = field.substring(0, field.indexOf("."));

		if (mappingField.contains(".")) {
			set.add(mappingField);
		} else {
			set.add(aliasName + "." + mappingField);
		}

		return set;
	}

	// TODO

//	public static Map<String,String> find 

//	public static Set<String> getTopLevelTableAlias(Map<String, TableLineageInfo> tableAliasLineageMap){
//		if(null==tableAliasLineageMap||0==tableAliasLineageMap.size()) {
//			return null;
//		}
//		
//		Collection<TableLineageInfo> infos=tableAliasLineageMap.values();
//		for(TableLineageInfo info:infos) {
//			if()
//		}
//	}
}
