package com.james.demo.jobflow;

public enum JobTypes {
    // business mysql to hive
    MYSQLTOHIVE("MYSQLTOHIVE", 0),
    // hive to mysql
    HIVETOMYSQL("HIVETOMYSQL", 1),
    // hive to vertica
    HIVETOVERTICA("HIVETOVERTICA", 2),
    // hive sql
    HIVE("HIVE", 3),
    // java job
    JAVAJOB("JAVAJOB", 4),
    // shell
    SHELL("SHELL", 5);

    private String name;
    private int index;

    private JobTypes(String name, int index) {
        this.name = name;
        this.index = index;
    }

    public static JobTypes fromIndex(int index) {
        return JobTypes.values()[index];
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
