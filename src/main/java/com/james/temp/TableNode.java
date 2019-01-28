package com.james.temp;

import java.util.List;

public class TableNode {
	private String tableName;
	private List<String> columns;

	public TableNode(){}

	public TableNode(String tableName,List<String> columns) {
		this.tableName=tableName;
		this.columns=columns;
	}
	
	public TableNode(String tableName) {
		this.tableName=tableName;
		this.columns=null;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	@Override
	public String toString() {
		return
				"(" + tableName +
				", " + columns +
				')';
	}
}

