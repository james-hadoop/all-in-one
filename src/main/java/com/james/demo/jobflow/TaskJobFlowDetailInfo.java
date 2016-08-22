package com.james.demo.jobflow;

import java.util.Date;

public class TaskJobFlowDetailInfo {
    private int id;
    private int jobFlowId;
    private String jobFlowName;
    private int taskSchedulerId;
    private String taskSchedulerName;
    private String description;
    private int jobId;
    private String jobName;
    private int jobType;
    private String parentJobId;
    private int step;
    private int stepItemOrder;
    private Date createTime;
    private int createUserId;

    private JobTypes jobTypeEnum;

    private Date batchTime;

    private String identifier;

    public String getIdentifier() {
        return this.getId() + "_" + this.getJobName() + "jobflowdetail";
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public Date getBatchTime() {
        return batchTime;
    }

    public void setBatchTime(Date batchTime) {
        this.batchTime = batchTime;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getJobType() {
        return jobType;
    }

    public void setJobType(int jobType) {
        this.jobType = jobType;
    }

    public JobTypes getJobTypeEnum() {
        return JobTypes.values()[this.getJobType()];
    }

    public void setJobTypeEnum(JobTypes jobTypeEnum) {
        this.jobTypeEnum = jobTypeEnum;
    }

    public String getParentJobId() {
        return parentJobId;
    }

    public void setParentJobId(String parentJobId) {
        this.parentJobId = parentJobId;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public int getStepItemOrder() {
        return stepItemOrder;
    }

    public void setStepItemOrder(int stepItemOrder) {
        this.stepItemOrder = stepItemOrder;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public int getCreateUserId() {
        return createUserId;
    }

    public void setCreateUserId(int createUserId) {
        this.createUserId = createUserId;
    }

    public int getJobFlowId() {
        return jobFlowId;
    }

    public void setJobFlowId(int jobFlowId) {
        this.jobFlowId = jobFlowId;
    }

    public String getJobFlowName() {
        return jobFlowName;
    }

    public void setJobFlowName(String jobFlowName) {
        this.jobFlowName = jobFlowName;
    }

    public int getTaskSchedulerId() {
        return taskSchedulerId;
    }

    public void setTaskSchedulerId(int taskSchedulerId) {
        this.taskSchedulerId = taskSchedulerId;
    }

    public String getTaskSchedulerName() {
        return taskSchedulerName;
    }

    public void setTaskSchedulerName(String taskSchedulerName) {
        this.taskSchedulerName = taskSchedulerName;
    }
}
