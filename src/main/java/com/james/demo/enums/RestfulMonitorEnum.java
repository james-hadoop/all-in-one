package com.james.demo.enums;

public enum RestfulMonitorEnum {
    NoLimit("不限", -1), Mornitoring("监控中", 0), Stopped("已停止", 1);

    private String value;
    private int code;

    private RestfulMonitorEnum(String value, int code) {
        this.value = value;
        this.code = code;
    }

    public static String getValue(int code) {
        for (RestfulMonitorEnum c : RestfulMonitorEnum.values()) {
            if (c.getCode() == code) {
                return c.value;
            }
        }
        return null;
    }

    public static int getCode(String value) {
        for (RestfulMonitorEnum c : RestfulMonitorEnum.values()) {
            if (c.getValue().equals(value)) {
                return c.code;
            }
        }
        return -1;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }
}
