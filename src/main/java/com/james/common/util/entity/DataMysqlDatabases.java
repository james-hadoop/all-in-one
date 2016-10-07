package com.james.common.util.entity;

public class DataMysqlDatabases {
    private Integer id;

    private String name;

    private String dbUrl;

    private String dbUsername;

    private String dbPwd;

    public DataMysqlDatabases() {

    }

    public DataMysqlDatabases(String name, String dbUrl, String dbUsername, String dbPwd) {
        this.name = name;
        this.dbUrl = dbUrl;
        this.dbUsername = dbUsername;
        this.dbPwd = dbPwd;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDbPwd() {
        return dbPwd;
    }

    public void setDbPwd(String dbPwd) {
        this.dbPwd = dbPwd;
    }

	public String getDbUsername() {
		return dbUsername;
	}

	public void setDbUsername(String dbUsername) {
		this.dbUsername = dbUsername;
	}
}