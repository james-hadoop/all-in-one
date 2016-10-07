package com.james.common.util.entity;

public class DataVerticaTable {
	// id in mysql table
	private Integer id;

	// id in vertica metadata table
	private Long tableId;

	private String strTableId;

	private String tableName;

	private Integer schemaId;

	private String name;

	private Integer status;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Long getTableId() {
		return tableId;
	}

	public void setTableId(Long tableId) {
		this.tableId = tableId;
	}

	public String getStrTableId() {
		return Long.toString(tableId);
	}

	public void setStrTableId(String strTableId) {
		this.strTableId = strTableId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Integer getSchemaId() {
		return schemaId;
	}

	public void setSchemaId(Integer schemaId) {
		this.schemaId = schemaId;
	}

	public Integer getStatus() {
		return status;
	}

	public void setStatus(Integer status) {
		this.status = status;
	}

	public String getName() {
		return tableName;
	}

	public void setName(String name) {
		this.name = name;
	}
}