package com.james.common.util.entity;

public class DataVerticaSchema {
	// id in mysql table
    private Integer id;
    
    // id in vertica metadata table
    private Long schemaId;
    
    private String strSchemaId;

    private String name;

    private Integer dbId;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }    

    public Long getSchemaId() {
		return schemaId;
	}

	public void setSchemaId(Long schemaId) {
		this.schemaId = schemaId;
	}

	public String getStrSchemaId() {
		return Long.toString(schemaId);
	}

	public void setStrSchemaId(String strSchemaId) {
		this.strSchemaId = strSchemaId;
	}

	public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getDbId() {
        return dbId;
    }

    public void setDbId(Integer dbId) {
        this.dbId = dbId;
    }
}