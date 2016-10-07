package com.james.common.util.entity;

import java.util.Date;

import com.james.common.util.DataUtil;
import com.james.common.util.HumanReadableUtils;

public class HiveDataTable extends MySqlHiveDBTable {
    private Integer id;

    private String dbName;

    private Integer dbId;

    private String hdfsPath;

    private String domain;

    private String description;

    private Integer syncType;

    private Integer partitionType;

    private String timeFormat;

    private String dayPartitionFormat;

    private String hourPartitionFormat;

    private Float alertThreshold;

    private Integer status;

    /**
     * 检查时间
     */
    private Date checkTime;
    /**
     * 偏离度
     */
    private Float deviation;

    private String deviationStr;
    /**
     * 表大小
     */
    private Long tableSize;

    private String sizeDescription;

    private Integer lastActionResult;

    private Date lastActionTime;

    public HiveDataTable(String name, int dbId, String hdfsPath, String description, int syncType) {
        this.name = name;
        this.dbId = dbId;
        this.hdfsPath = hdfsPath;
        this.description = description;
        this.syncType = syncType;
    }

    public HiveDataTable() {

    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getDbId() {
        return dbId;
    }

    public void setDbId(Integer dbId) {
        this.dbId = dbId;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getSyncType() {
        return syncType;
    }

    public void setSyncType(Integer syncType) {
        this.syncType = syncType;
    }

    public Integer getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(Integer partitionType) {
        this.partitionType = partitionType;
    }

    public String getDayPartitionFormat() {
        return dayPartitionFormat;
    }

    public void setDayPartitionFormat(String dayPartitionFormat) {
        this.dayPartitionFormat = dayPartitionFormat;
    }

    public String getHourPartitionFormat() {
        return hourPartitionFormat;
    }

    public void setHourPartitionFormat(String hourPartitionFormat) {
        this.hourPartitionFormat = hourPartitionFormat;
    }

    public Float getAlertThreshold() {
        return alertThreshold;
    }

    public void setAlertThreshold(Float alertThreshold) {
        this.alertThreshold = alertThreshold;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Date getCheckTime() {
        return checkTime;
    }

    public void setCheckTime(Date checkTime) {
        this.checkTime = checkTime;
    }

    public Float getDeviation() {
        return deviation;
    }

    public void setDeviation(Float deviation) {
        this.deviation = deviation;
        String valueStr = String.valueOf(deviation * 100);
        double value = DataUtil.getDoubleWith4AfterPoint(Double.valueOf(valueStr));
        this.deviationStr = String.valueOf(value);
    }

    public Long getTableSize() {
        return tableSize;
    }

    public void setTableSize(Long tableSize) {
        this.sizeDescription = HumanReadableUtils.byteSize(tableSize);
        this.tableSize = tableSize;
    }

    public Integer getLastActionResult() {
        return lastActionResult;
    }

    public void setLastActionResult(Integer lastActionResult) {
        this.lastActionResult = lastActionResult;
    }

    public Date getLastActionTime() {
        return lastActionTime;
    }

    public void setLastActionTime(Date lastActionTime) {
        this.lastActionTime = lastActionTime;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getSizeDescription() {
        return sizeDescription;
    }

    public void setSizeDescription(String sizeDescription) {
        this.sizeDescription = sizeDescription;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        if (timeFormat.contains("=")) {
            timeFormat = timeFormat.substring(timeFormat.indexOf("=") + 1);
        }
        this.timeFormat = timeFormat;
    }

    public String getDeviationStr() {
        return deviationStr;
    }

    public void setDeviationStr(String deviationStr) {
        this.deviationStr = deviationStr;
    }
}