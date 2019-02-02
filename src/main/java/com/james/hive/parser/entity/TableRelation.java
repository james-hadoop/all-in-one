package com.james.hive.parser.entity;

import java.util.List;
import java.util.Map;

public class TableRelation {
	private List<TableNode> sources;
	private TableNode target;
	private Map<String, String> columnRelations;

	public TableRelation() {
	}

	public TableRelation(List<TableNode> sources, TableNode target, Map<String, String> columnRelations) {
		this.sources = sources;
		this.target = target;
		this.columnRelations = columnRelations;
	}

	public TableRelation(List<TableNode> sources, TableNode target) {
		this.sources = sources;
		this.target = target;
		columnRelations = null;
	}

	public List<TableNode> getSources() {
		return sources;
	}

	public void setSources(List<TableNode> sources) {
		this.sources = sources;
	}

	public TableNode getTarget() {
		return target;
	}

	public void setTarget(TableNode target) {
		this.target = target;
	}

	public Map<String, String> getColumnRelations() {
		return columnRelations;
	}

	public void setColumnRelations(Map<String, String> columnRelations) {
		this.columnRelations = columnRelations;
	}

	@Override
	public String toString() {
		return "TableRelation{" + target + "," + sources + "," + columnRelations + '}';
	}
}