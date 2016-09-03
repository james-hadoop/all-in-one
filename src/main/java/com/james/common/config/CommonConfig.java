package com.james.common.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonConfig {
    private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);
    private static final String CONFIG_FILE = "config" + File.separator + "common.properties";

    private String mailFrom;
    private String mailHost;
    private String mailUserName;
    private String mailPassword;
    private String mailPort;

    private static volatile CommonConfig instance;

    static {
        instance = new CommonConfig();
    }

    public static CommonConfig getInstance() {
        return instance;
    }

    public CommonConfig() {
        this.loadingConfig();
    }

    private void loadingConfig() {
        Properties props = new Properties();
        try {
            FileInputStream fis = new FileInputStream(CONFIG_FILE);
            props.load(fis);
            fis.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error in loading configuration");
        }

        this.mailFrom = props.getProperty("common.mail_from", "");
        this.mailHost = props.getProperty("common.mail_host", "");
        this.mailUserName = props.getProperty("common.mail_username", "");
        this.mailPassword = props.getProperty("ames@Fangdd2016", "");
        this.mailPort = props.getProperty("common.mail_port", "");
    }

    public String getMailFrom() {
        return mailFrom;
    }

    public void setMailFrom(String mailFrom) {
        this.mailFrom = mailFrom;
    }

    public String getMailHost() {
        return mailHost;
    }

    public void setMailHost(String mailHost) {
        this.mailHost = mailHost;
    }

    public String getMailUserName() {
        return mailUserName;
    }

    public void setMailUserName(String mailUserName) {
        this.mailUserName = mailUserName;
    }

    public String getMailPassword() {
        return mailPassword;
    }

    public void setMailPassword(String mailPassword) {
        this.mailPassword = mailPassword;
    }

    public String getMailPort() {
        return mailPort;
    }

    public void setMailPort(String mailPort) {
        this.mailPort = mailPort;
    }
}
