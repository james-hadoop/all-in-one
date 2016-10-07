package com.james.common.util;

public class SqlUtil {
    public static String buildSqlQuery(String dbName, String tableName, String tableFields, String tableCondition,
            String dividerAmongFields, String dividerBetweenFieldAndType) {
        if (null == tableFields || 0 == tableFields.length()) {
            return null;
        }

        StringBuilder sql = new StringBuilder();
        try {
            StringBuilder fields = new StringBuilder();

            String[] arrFieldTypeTuple = tableFields.split(dividerAmongFields);
            if (null == arrFieldTypeTuple || 0 == arrFieldTypeTuple.length) {
                fields.append("*");
            } else {
                int nSize = arrFieldTypeTuple.length;
                for (int i = 0; i < nSize; i++) {
                    String[] arrFieldOrType = arrFieldTypeTuple[i].split(dividerBetweenFieldAndType);
                    fields.append(arrFieldOrType[0]);
                    if (i != nSize - 1) {
                        fields.append(",");
                    }
                }
            }

            sql.append("select ");
            sql.append(fields.toString());
            sql.append(" from ");
            sql.append(dbName + "." + tableName);
            if (null != tableCondition && 0 != tableCondition.length()) {
                sql.append(" where ");
                sql.append(tableCondition);
            }
        } catch (Exception e) {
            throw e;
        }

        return sql.toString();
    }
}
