package com.james.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveJdbcTest4 {
    private static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(HIVE_JDBC_DRIVER);
        Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/mydb", "anonymous", "anonymous");

        String sql = null;
        ResultSet rs = null;
        ResultSetMetaData resultMetaData = null;
        PreparedStatement pstmt = conn.prepareStatement(sql);

        String tableName = "employees2;";

        // read data from Hive table
        sql = "select * from " + tableName;
        System.out.println("Running:\n\t" + sql);
        rs = pstmt.executeQuery(sql);
        resultMetaData = rs.getMetaData();
        int nSize = resultMetaData.getColumnCount();

        List<List<Object>> listListRows = new ArrayList<List<Object>>();
        while (rs.next()) {
            List<Object> listOneRow = new ArrayList<Object>();
            for (int i = 1; i < nSize; i++) {
                switch (resultMetaData.getColumnType(i)) {
                case Types.VARCHAR:
                    listOneRow.add(rs.getString(resultMetaData.getColumnName(i)));
                    break;
                case Types.INTEGER:
                    listOneRow.add(new Integer(rs.getInt(resultMetaData.getColumnName(i))));
                    break;
                case Types.BIGINT:
                    listOneRow.add(new Long(rs.getLong(resultMetaData.getColumnName(i))));
                    break;
                case Types.TIMESTAMP:
                    listOneRow.add(rs.getDate(resultMetaData.getColumnName(i)));
                    break;
                case Types.DOUBLE:
                    listOneRow.add(rs.getDouble(resultMetaData.getColumnName(i)));
                    break;
                case Types.FLOAT:
                    listOneRow.add(rs.getFloat(resultMetaData.getColumnName(i)));
                    break;
                case Types.CLOB:
                    listOneRow.add(rs.getBlob(resultMetaData.getColumnName(i)));
                    break;
                default:
                    listOneRow.add("unknown data type");
                }
            }
            listListRows.add(listOneRow);
        }

        System.out.println(listListRows.size());
        for (List<Object> oneRow : listListRows) {
            for (Object column : oneRow) {
                System.out.print(column + "\t");
            }
            System.out.println();
        }
        printDivider();
    }

    private static void printDivider() {
        System.out.println("------------------------------------------------------------------------\n");
    }

    private static Map<String, String> getTableFields(Connection conn, String tableName) throws SQLException {
        if (null == conn || conn.isClosed()) {
            return null;
        }

        if (null == tableName || 0 == tableName.length()) {
            return null;
        }

        Map<String, String> mapTableFields = new HashMap<String, String>();

        final String sql = "describe " + tableName;

        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery(sql);
        while (rs.next()) {
            mapTableFields.put(rs.getString(1), rs.getString(2));
        }

        return mapTableFields;
    }

    private static boolean createTable(Connection conn, String tableName) throws SQLException {
        if (null == conn || conn.isClosed()) {
            return false;
        }

        if (null == tableName || 0 == tableName.length()) {
            return false;
        }

        return true;
    }

    private static boolean dropTable(Connection conn, String tableName) throws SQLException {
        if (null == conn || conn.isClosed()) {
            return false;
        }

        if (null == tableName || 0 == tableName.length()) {
            return false;
        }

        return true;
    }

    private static boolean renameTable(Connection conn, String tableName) throws SQLException {
        if (null == conn || conn.isClosed()) {
            return false;
        }

        if (null == tableName || 0 == tableName.length()) {
            return false;
        }

        return true;
    }

    private Map<String, String> parseTableFields(String tableFields, String dividerAmongFields,
            String dividerBetweenFieldAndType) {
        if (null == tableFields || 0 == tableFields.length()) {
            return null;
        }

        Map<String, String> mapFieldType = new HashMap<String, String>();
        String[] arrFieldTypeTuple = tableFields.split(dividerAmongFields);
        if (null == arrFieldTypeTuple || 0 == arrFieldTypeTuple.length) {
            return null;
        }

        int nSize = arrFieldTypeTuple.length;
        for (int i = 0; i < nSize; i++) {
            String[] arrFieldOrType = arrFieldTypeTuple[i].split(dividerBetweenFieldAndType);
            mapFieldType.put(arrFieldOrType[0], arrFieldOrType[1]);
        }

        return mapFieldType;
    }
}
