package com.james.common.util.entity;

public class DataMysqlTable {
    private Integer id;

    private String tableName;

    private Integer dbId;

    private String dbName;

    private Integer status;

    private String name;

    public DataMysqlTable() {

    }

    public DataMysqlTable(String tableName) {
        this.tableName = tableName;
    }

    public DataMysqlTable(String tableName, int dbId, int status) {
        this.tableName = tableName;
        this.dbId = dbId;
        this.status = status;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getDbId() {
        return dbId;
    }

    public void setDbId(Integer dbId) {
        this.dbId = dbId;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getName() {
        return tableName;
    }

    public void setName(String name) {
        this.name = name;
    }
}