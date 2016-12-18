package com.james.mail.amazon.parser.entity;

public class CardEntity {
    private String code;
    private String money;
    private String unit;
    private String country;

    public CardEntity() {

    }

    public CardEntity(String code, String money, String unit, String country) {
        this.code = code;
        this.money = money;
        this.unit = unit;
        this.country = country;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMoney() {
        return money;
    }

    public void setMoney(String money) {
        this.money = money;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        country = country;
    }
}