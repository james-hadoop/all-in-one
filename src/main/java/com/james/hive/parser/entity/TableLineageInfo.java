package com.james.hive.parser.entity;

import java.util.Map;
import java.util.Map.Entry;

public class TableLineageInfo {
	private String tableAliasName;
	private Map<String,String> tableAliasReferMap;
	
	public TableLineageInfo() {
		
	}
	
	public TableLineageInfo(String tableAliasName,Map<String,String> tableAliasReferMap) {
		this.tableAliasName=tableAliasName;
		this.tableAliasReferMap=tableAliasReferMap;
	}
	
	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append(tableAliasName+":\n");
		
		for(Entry<String, String> e:tableAliasReferMap.entrySet()) {
			sb.append("\t"+e.getKey()+ " -> "+e.getValue()+"\n");
		}
		
		return sb.toString();
	}

	public String getTableAliasName() {
		return tableAliasName;
	}

	public void setTableAliasName(String tableAliasName) {
		this.tableAliasName = tableAliasName;
	}

	public Map<String, String> getTableAliasReferMap() {
		return tableAliasReferMap;
	}

	public void setTableAliasReferMap(Map<String, String> tableAliasReferMap) {
		this.tableAliasReferMap = tableAliasReferMap;
	}
}
