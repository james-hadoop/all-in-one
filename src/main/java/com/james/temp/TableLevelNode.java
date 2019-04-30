package com.james.temp;

import java.util.Map;

import org.apache.hadoop.hive.ql.parse.HiveParser;

public class TableLevelNode {
	private String tableName;
	private int level;
	private TableLevelNode parent;

	public TableLevelNode(String tableName, int level, TableLevelNode parent) {
		this.tableName = tableName;
		this.level = level;
		this.parent = parent;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public TableLevelNode getParent() {
		return parent;
	}

	public void setParent(TableLevelNode parent) {
		this.parent = parent;
	}
}
