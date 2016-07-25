package com.james.demo.configure;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JamesConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(JamesConfig.class);
    private static final String CONFIG_FILE = "config" + File.separator + "config.properties";

    private String mysqlJdbcDriver;
    private String mysqlJdbcUrl;
    private String mysqlJdbcUsername;
    private String mysqlJdbcPassword;
    private int poolMaxSize;
    private int poolMinSize;

    private static volatile JamesConfig instance;

    static {
        instance = new JamesConfig();
    }

    public static JamesConfig getInstance() {
        return instance;
    }

    public JamesConfig() {
        this.loadingConfig();
    }

    private void loadingConfig() {
        Properties props = new Properties();
        try {
            FileInputStream fis = new FileInputStream(CONFIG_FILE);
            props.load(fis);
            fis.close();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException("Error in loading configuration");
        }

        this.mysqlJdbcDriver = props.getProperty("mysql.jdbc.driver", "");
        this.mysqlJdbcUrl = props.getProperty("mysql.jdbc.url", "");
        this.mysqlJdbcUsername = props.getProperty("mysql.jdbc.username", "");
        this.mysqlJdbcPassword = props.getProperty("mysql.jdbc.password", "");
        this.poolMaxSize = Integer.parseInt(props.getProperty("poolMaxSize", ""));
        this.poolMinSize = Integer.parseInt(props.getProperty("poolMinSize", ""));
    }

    public String getMysqlJdbcDriver() {
        return mysqlJdbcDriver;
    }

    public void setMysqlJdbcDriver(String mysqlJdbcDriver) {
        this.mysqlJdbcDriver = mysqlJdbcDriver;
    }

    public String getMysqlJdbcUrl() {
        return mysqlJdbcUrl;
    }

    public void setMysqlJdbcUrl(String mysqlJdbcUrl) {
        this.mysqlJdbcUrl = mysqlJdbcUrl;
    }

    public String getMysqlJdbcUsername() {
        return mysqlJdbcUsername;
    }

    public void setMysqlJdbcUsername(String mysqlJdbcUsername) {
        this.mysqlJdbcUsername = mysqlJdbcUsername;
    }

    public String getMysqlJdbcPassword() {
        return mysqlJdbcPassword;
    }

    public void setMysqlJdbcPassword(String mysqlJdbcPassword) {
        this.mysqlJdbcPassword = mysqlJdbcPassword;
    }

    public int getPoolMaxSize() {
        return poolMaxSize;
    }

    public void setPoolMaxSize(int poolMaxSize) {
        this.poolMaxSize = poolMaxSize;
    }

    public int getPoolMinSize() {
        return poolMinSize;
    }

    public void setPoolMinSize(int poolMinSize) {
        this.poolMinSize = poolMinSize;
    }

    public static void main(String[] args) {
        JamesConfig config = JamesConfig.getInstance();
        System.out.println(config.getMysqlJdbcDriver());
        System.out.println(config.getMysqlJdbcUrl());
        System.out.println(config.getMysqlJdbcUsername());
        System.out.println(config.getMysqlJdbcPassword());
        System.out.println(config.getPoolMaxSize());
        System.out.println(config.getPoolMinSize());
    }
}
