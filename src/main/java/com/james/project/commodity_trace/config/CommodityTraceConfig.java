package com.james.project.commodity_trace.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommodityTraceConfig {
    private static final Logger logger = LoggerFactory.getLogger(CommodityTraceConfig.class);
    private static final String CONFIG_FILE = "config" + File.separator + "commodity_trace.properties";

    private String url;
    private String threshold;
    private String unit;
    private String mailList;
    private int interval;
    private String priceMark;
    private String priceEndMark;
    private String comma;
    private String dot;

    private static volatile CommodityTraceConfig instance;

    static {
        instance = new CommodityTraceConfig();
    }

    public static CommodityTraceConfig getInstance() {
        return instance;
    }

    public CommodityTraceConfig() {
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

        this.url = props.getProperty("commodity.trace.url", "");
        this.threshold = props.getProperty("commodity.trace.threshold", "");
        this.unit = props.getProperty("commodity.trace.unit", "");
        this.mailList = props.getProperty("commodity.trace.mailList", "");
        this.interval = Integer.valueOf(props.getProperty("commodity.trace.interval", ""));
        this.priceMark = props.getProperty("commodity.trace.price_mark", "");
        this.priceEndMark = props.getProperty("commodity.trace.price_end_mark", "");
        this.comma = props.getProperty("commodity.trace.comma", "");
        this.dot = props.getProperty("commodity.trace.dot", "");
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getThreshold() {
        return threshold;
    }

    public void setThreshold(String threshold) {
        this.threshold = threshold;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getMailList() {
        return mailList;
    }

    public void setMailList(String mailList) {
        this.mailList = mailList;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public String getPriceMark() {
        return priceMark;
    }

    public void setPriceMark(String priceMark) {
        this.priceMark = priceMark;
    }

    public String getPriceEndMark() {
        return priceEndMark;
    }

    public void setPriceEndMark(String priceEndMark) {
        this.priceEndMark = priceEndMark;
    }

    public String getComma() {
        return comma;
    }

    public void setComma(String comma) {
        this.comma = comma;
    }

    public String getDot() {
        return dot;
    }

    public void setDot(String dot) {
        this.dot = dot;
    }
}
