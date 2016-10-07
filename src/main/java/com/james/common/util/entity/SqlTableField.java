package com.james.common.util.entity;

public class SqlTableField {
    private String fieldName;
    private String type;
    private String comments;

    public SqlTableField() {

    }

    public SqlTableField(String fieldName, String type, String comments) {
        this.fieldName = fieldName;
        this.type = type;
        this.comments = comments;
    }

    public String toString() {
        // house_id int comment '房源ID PK 原始数据类型:int(20)',
        return fieldName + " " + type + " comment '" + comments.replace("'", "") + " 原始数据类型:" + type+"'";
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }
}
