package com.james.mail.amazon.parser.entity;

public enum CountryEnum {
    it("it", 0), jp("jp", 1), de("de", 2), fr("fr", 3),

    ca("ca", 11), es("es", 12), uk("uk", 13), com("com", 14);

    private String value;
    private int code;

    private CountryEnum(String value, int code) {
        this.value = value;
        this.code = code;
    }

    public static CountryEnum getByCode(int code) {
        return CountryEnum.values()[code];
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