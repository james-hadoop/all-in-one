package com.james.common.util.entity;

public class DataTableColumnEntity {
	// id in vertica meta table
	private String columnId;
	
	private String name;
	
	private String type;
	
	private long length;
	
	public DataTableColumnEntity(){
		
	}

	public String getColumnId() {
		return columnId;
	}
	
	public void setColumnId(String columnId) {
		this.columnId = columnId;
	}



	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}	
}
