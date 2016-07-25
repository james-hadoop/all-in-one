package com.james.demo.enums;

public enum RestfulCheckEnum {
    NoLimit("不限", -1), Normal("正常", 0), Abnormal("异常", 1);

    private String value;
    private int code;

    private RestfulCheckEnum(String value, int code) {
        this.value = value;
        this.code = code;
    }

    public static String getValue(int code) {
        for (RestfulCheckEnum c : RestfulCheckEnum.values()) {
            if (c.getCode() == code) {
                return c.value;
            }
        }
        return null;
    }

    public static int getCode(String value) {
        for (RestfulCheckEnum c : RestfulCheckEnum.values()) {
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
