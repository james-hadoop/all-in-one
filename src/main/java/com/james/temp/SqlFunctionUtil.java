package com.james.temp;

public class SqlFunctionUtil {

    public static void main(String[] args) {
        String s1 = "r_t_a.SUM(MAX(r_a_a))";
        String s2 = "r_t_a.MAX(r_a_a)";
        String s3 = "(MAX(r_a_a))";
        String s4 = "nvl(a.h,b.i)";
        String s5 = "hello";
        String s6 = "nvl(a,b)";
        String s7 = "james.hi";

        String sqlFunction = s2;

        System.out.println("before: " + sqlFunction);
        System.out.println("after:  " + removeSqlFunctionName(sqlFunction));
    }

    /*
     * hello -> hello
     * 
     * r_t_a.SUM(MAX(r_a_a)) -> r_t_a.r_a_a
     * 
     * nvl(a,b) -> a,b
     * 
     */

    public static String removeSqlFunctionName(String sqlFunction) {
        if (null == sqlFunction || 0 == sqlFunction.length()) {
            return null;
        }

        String tableName = getTableName(sqlFunction);
        String tableData = getTableData(sqlFunction);

        String tableCol = removeBrackets(tableData);

//        System.out.println(tableName);
//        System.out.println(tableData);
//        System.out.println(tableCol);

        if (tableName.equals(tableData)) {
            return tableCol;
        }

        return tableName + "." + tableCol;
    }

    private static String removeBrackets(String content) {
        if (null == content || 0 == content.length()) {
            return null;
        }

        if (!content.contains("(")) {
            return content;
        }

        return content.substring(content.lastIndexOf("(") + 1, content.indexOf(")"));
    }

    public static String getTableName(String content) {
        if (null == content || 0 == content.length()) {
            return null;
        }

        if (!content.contains(".")) {
            return content;
        }

        if (-1 != content.indexOf("(") && content.indexOf("(") < content.indexOf(".")) {
            return content;
        }

        return content.substring(0, content.indexOf("."));
    }

    public static String getTableData(String content) {
        if (null == content || 0 == content.length()) {
            return null;
        }

        if (!content.contains(".")) {
            return content;
        }

        if (-1 != content.indexOf("(") && content.indexOf("(") < content.indexOf(".")) {
            return content;
        }

        return content.substring(content.indexOf(".") + 1, content.length());
    }
}
