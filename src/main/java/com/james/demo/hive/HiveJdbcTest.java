package com.james.demo.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcTest {
    private static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(HIVE_JDBC_DRIVER);
        Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/logs", "", "");

        Statement stmt = conn.createStatement();
        String sql = null;
        ResultSet rs = null;

        String tableName = "test";

        // show tables
        sql = "show tables '" + tableName + "'";
        System.out.println("Running:\n\t" + sql);
        rs = stmt.executeQuery(sql);
        if (rs.next()) {
            System.out.println(rs.getString(1));
        }
        printDivider();

        // describe table
        sql = "describe " + tableName;
        System.out.println("Running:\n\t" + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
        printDivider();

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running:\n\t" + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString("name") + "\t" + rs.getInt("age") + "\t" + rs.getFloat("gpa"));
        }
        printDivider();

        sql = "select count(1) from " + tableName;
        System.out.println("Running:\n\t" + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));

        }
    }

    private static void printDivider() {
        System.out.println("------------------------------------------------------------------------\n");
    }
}
