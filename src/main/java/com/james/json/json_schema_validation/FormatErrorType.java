package com.james.json.json_schema_validation;

public enum FormatErrorType {
    Good("0", 0), ContainReservedCharacter("1", 1), InvalidJsonSchema("2", 2);

    private String value;
    private int code;

    private FormatErrorType(String value, int code) {
        this.value = value;
        this.code = code;
    }

    public static String getValue(int code) {
        for (FormatErrorType c : FormatErrorType.values()) {
            if (c.getCode() == code) {
                return c.value;
            }
        }
        return null;
    }

    public static int getCode(String value) {
        for (FormatErrorType c : FormatErrorType.values()) {
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