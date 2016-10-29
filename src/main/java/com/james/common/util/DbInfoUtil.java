package com.james.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.james.common.util.entity.DataMysqlDatabases;
import com.james.common.util.entity.DataMysqlTable;
import com.james.common.util.entity.DataTableColumnEntity;
import com.james.common.util.entity.DataVerticaDatabases;
import com.james.common.util.entity.DataVerticaSchema;
import com.james.common.util.entity.DataVerticaTable;
import com.james.common.util.entity.HiveDataTable;

public class DbInfoUtil {
    private static final Logger logger = LoggerFactory.getLogger(DbInfoUtil.class);

    private static final String MYSQL_DB_DRIVER = "";
    private static final String VERTICA_DB_DRIVER = "";
    private static final String HIVE_META_DB_URL = "";
    private static final String HIVE_META_DB_USERNAME = "";
    private static final String HIVE_META_DB_PASSWORD = "";

    private static final byte[] key = "my_encrypt_key".getBytes();

    public static List<DataVerticaSchema> getVerticaSchemaList(DataVerticaDatabases db, Long schemaId) throws Exception {
        List<DataVerticaSchema> listDataVerticaSchema = new ArrayList<DataVerticaSchema>();

        String sql = null;
        ResultSet rs = null;

        Connection connVertica = null;
        try {
            Class.forName(VERTICA_DB_DRIVER);

            connVertica = DriverManager.getConnection(db.getDbUrl(), db.getDbUsername(),
                    DataUtil.decrypt(db.getDbPwd(), key));
            if (null == connVertica) {
                logger.warn("DbInfoHelper/getVerticaSchemaList() fail: cannot get database connection.");
                return null;
            }

            if (null == schemaId) {
                sql = "select schema_id, schema_name from v_catalog.schemata";
            } else {
                sql = "select schema_id, schema_name from v_catalog.schemata where schema_id=" + schemaId;
            }

            Statement stmt = connVertica.createStatement();
            rs = stmt.executeQuery(sql);

            while (rs.next()) {
                DataVerticaSchema vs = new DataVerticaSchema();
                vs.setSchemaId(rs.getLong("schema_id"));
                vs.setName(rs.getString("schema_name"));

                listDataVerticaSchema.add(vs);
            }
        } catch (Exception e) {
            logger.warn("DbInfoHelper/getVerticaSchemaList() fail.");
            throw e;
        } finally {
            connVertica.close();
        }

        return listDataVerticaSchema;
    }

    public static List<DataVerticaTable> getVerticaTableList(DataVerticaDatabases db, Long schemaId, Long tableId)
            throws Exception {
        List<DataVerticaTable> listDataVerticaTable = new ArrayList<DataVerticaTable>();

        ResultSet rs = null;
        String sql;

        Connection connVertica = null;
        try {
            Class.forName(VERTICA_DB_DRIVER);

            connVertica = DriverManager.getConnection(db.getDbUrl(), db.getDbUsername(),
                    DataUtil.decrypt(db.getDbPwd(), key));
            if (null == connVertica) {
                logger.warn("DbInfoHelper/getVerticaTableList() fail: cannot get database connection.");
                return null;
            }

            if (null == schemaId) {
                sql = "select table_id, table_schema, table_name from v_catalog.tables";

                if (null != tableId) {
                    sql += " where talbe_id='" + tableId + "'";
                }
            } else {
                sql = "select table_id, table_schema, table_name from v_catalog.tables where table_schema_id='"
                        + schemaId + "'";

                if (null != tableId) {
                    sql += " and talbe_id='" + tableId + "'";
                }
            }

            Statement stmt = connVertica.createStatement();
            rs = stmt.executeQuery(sql);

            while (rs.next()) {
                DataVerticaTable vt = new DataVerticaTable();
                vt.setTableId(rs.getLong("table_id"));
                vt.setTableName(rs.getString("table_name"));

                listDataVerticaTable.add(vt);
            }
        } catch (Exception e) {
            logger.warn("DbInfoHelper/getVerticaTableList() fail.");
            throw e;
        } finally {
            connVertica.close();
        }

        return listDataVerticaTable;
    }

    public static List<DataTableColumnEntity> getVerticaTableColumnList(DataVerticaDatabases db, Long tableId)
            throws Exception {
        List<DataTableColumnEntity> listDataTableColumnEntity = new ArrayList<DataTableColumnEntity>();

        if (null == db || null == tableId) {
            logger.warn("DbInfoHelper/getVerticaTableColumnList() fail: parameters are null.");
            return listDataTableColumnEntity;
        }

        ResultSet rs = null;
        String sql;

        Connection connVertica = null;
        try {
            Class.forName(VERTICA_DB_DRIVER);

            connVertica = DriverManager.getConnection(db.getDbUrl(), db.getDbUsername(),
                    DataUtil.decrypt(db.getDbPwd(), key));
            if (null == connVertica) {
                logger.warn("DbInfoHelper/getVerticaTableColumnList() fail: cannot get database connection.");
                return null;
            }

            sql = "select column_id, column_name,data_type,data_type_length from v_catalog.columns where table_id='"
                    + tableId + "'";

            Statement stmt = connVertica.createStatement();
            rs = stmt.executeQuery(sql);

            while (rs.next()) {
                DataTableColumnEntity vc = new DataTableColumnEntity();
                vc.setColumnId(rs.getString("column_id"));
                vc.setName(rs.getString("column_name"));
                vc.setType(rs.getString("data_type"));
                vc.setLength(rs.getLong("data_type_length"));

                listDataTableColumnEntity.add(vc);
            }
        } catch (Exception e) {
            logger.warn("DbInfoHelper/getVerticaTableColumnList() fail.");
            throw e;
        } finally {
            connVertica.close();
        }

        return listDataTableColumnEntity;
    }

    public static List<DataMysqlTable> getMysqlTableList(DataMysqlDatabases db) throws Exception {
        List<DataMysqlTable> listDataMysqlTable = new ArrayList<DataMysqlTable>();

        String sql = null;

        Connection connMysql = null;
        try {
            Class.forName(MYSQL_DB_DRIVER);

            connMysql = DriverManager.getConnection(db.getDbUrl(), db.getDbUsername(),
                    DataUtil.decrypt(db.getDbPwd(), key));
            if (null == connMysql) {
                logger.warn("DbInfoHelper/getMysqlTableList() fail: cannot get database connection.");
                return null;
            }

            sql = "select table_name from information_schema.tables where table_schema='" + db.getName() + "'";

            PreparedStatement pstmt = connMysql.prepareStatement(sql);

            ResultSet rs = pstmt.executeQuery(sql);

            while (rs.next()) {
                DataMysqlTable mt = new DataMysqlTable();
                mt.setDbId(db.getId());
                mt.setTableName(rs.getString("table_name"));

                listDataMysqlTable.add(mt);
            }
        } catch (Exception e) {
            logger.warn("DbInfoHelper/getMysqlTableList() fail.");
            throw e;
        } finally {
            connMysql.close();
        }

        return listDataMysqlTable;
    }

    public static List<DataTableColumnEntity> getMysqlTableColumnList(DataMysqlDatabases db, String tableName)
            throws Exception {
        List<DataTableColumnEntity> listDataTableColumnEntity = new ArrayList<DataTableColumnEntity>();

        if (null == db || null == tableName || 0 == tableName.length()) {
            logger.warn("DbInfoHelper/getMysqlTableColumnList() fail: parameters are null.");
            return listDataTableColumnEntity;
        }

        String sql = null;

        Connection connMysql = null;
        try {
            Class.forName(MYSQL_DB_DRIVER);

            connMysql = DriverManager.getConnection(db.getDbUrl(), db.getDbUsername(),
                    DataUtil.decrypt(db.getDbPwd(), key));
            if (null == connMysql) {
                logger.warn("DbInfoHelper/getMysqlTableList() fail: cannot get database connection.");
                return null;
            }

            sql = "select column_name,column_type from information_schema.columns where table_schema='" + db.getName()
                    + "' and table_name='" + tableName + "'";

            PreparedStatement pstmt = connMysql.prepareStatement(sql);

            ResultSet rs = pstmt.executeQuery(sql);

            while (rs.next()) {
                DataTableColumnEntity vc = new DataTableColumnEntity();
                vc.setName(rs.getString("column_name"));
                vc.setType(rs.getString("column_type"));

                listDataTableColumnEntity.add(vc);
            }
        } catch (Exception e) {
            logger.warn("DbInfoHelper/getMysqlTableList() fail.");
            throw e;
        } finally {
            connMysql.close();
        }

        return listDataTableColumnEntity;
    }

    public static List<HiveDataTable> getHiveTableList(String dbName) throws Exception {
        List<HiveDataTable> listHiveDataTable = new ArrayList<HiveDataTable>();

        if (null == dbName || 0 == dbName.length()) {
            logger.warn("DbInfoHelper/getHiveTableList() fail: parameters are null.");
            return listHiveDataTable;
        }

        String sql = null;

        Connection connMysql = null;
        try {
            Class.forName(MYSQL_DB_DRIVER);

            connMysql = DriverManager.getConnection(HIVE_META_DB_URL, HIVE_META_DB_USERNAME,
                    DataUtil.decrypt(HIVE_META_DB_PASSWORD, key));
            if (null == connMysql) {
                logger.warn("DbInfoHelper/getHiveTableList() fail: cannot get database connection.");
                return null;
            }

            sql = "select tbl_name from TBLS where db_id=(select db_id from DBS where name=?)";

            PreparedStatement pstmt = connMysql.prepareStatement(sql);
            pstmt.setObject(1, dbName);

            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                HiveDataTable ht = new HiveDataTable();
                ht.setName(rs.getString("tbl_name"));

                listHiveDataTable.add(ht);
            }
        } catch (Exception e) {
            logger.warn("DbInfoHelper/getHiveTableList() fail.");
            throw e;
        } finally {
            connMysql.close();
        }

        return listHiveDataTable;
    }

    public static List<DataTableColumnEntity> getHiveTableColumnList(String dbName, String tableName) throws Exception {
        List<DataTableColumnEntity> listDataTableColumnEntity = new ArrayList<DataTableColumnEntity>();

        if (null == dbName || 0 == dbName.length() || null == tableName || 0 == tableName.length()) {
            logger.warn("DbInfoHelper/getHiveTableColumnList() fail: parameters are null.");
            return listDataTableColumnEntity;
        }

        String sql = null;

        Connection connMysql = null;
        try {
            Class.forName(MYSQL_DB_DRIVER);

            connMysql = DriverManager.getConnection(HIVE_META_DB_URL, HIVE_META_DB_USERNAME,
                    DataUtil.decrypt(HIVE_META_DB_PASSWORD, key));
            if (null == connMysql) {
                logger.warn("DbInfoHelper/getHiveTableColumnList() fail: cannot get database connection.");
                return null;
            }

            sql = "select column_name, type_name from DBS as d" + " left outer join" + " TBLS as t"
                    + " on d.db_id=t.db_id" + " left outer join" + " COLUMNS_V2 as c" + " on c.cd_id=t.tbl_id"
                    + " where d.name='" + dbName + "' and t.tbl_name='" + tableName + "' order by INTEGER_IDX";

            PreparedStatement pstmt = connMysql.prepareStatement(sql);

            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                DataTableColumnEntity vc = new DataTableColumnEntity();
                vc.setName(rs.getString("column_name"));
                vc.setType(rs.getString("type_name"));

                listDataTableColumnEntity.add(vc);
            }
        } catch (Exception e) {
            logger.warn("DbInfoHelper/getHiveTableColumnList() fail.");
            throw e;
        } finally {
            connMysql.close();
        }

        return listDataTableColumnEntity;
    }

    public static List<String> getAllHiveDBInfo() throws Exception {
        List<String> dblist = new ArrayList<String>();
        Connection connMysql = null;
        try {
            Class.forName(MYSQL_DB_DRIVER);

            connMysql = DriverManager.getConnection(HIVE_META_DB_URL, HIVE_META_DB_USERNAME,
                    DataUtil.decrypt(HIVE_META_DB_PASSWORD, key));
            if (null == connMysql) {
                logger.warn("DbInfoHelper/getAllHiveDBInfo() fail: cannot get database connection.");
                return null;
            }
            String sql = "select name as tableName from DBS";
            PreparedStatement pstmt = connMysql.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString("tableName");
                dblist.add(tableName);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connMysql.close();
        }
        return dblist;
    }

    public static List<String> getHiveColumnInfo(String tablename) throws Exception {
        List<String> columns = new ArrayList<String>();
        Connection connMysql = null;
        try {
            Class.forName(MYSQL_DB_DRIVER);

            connMysql = DriverManager.getConnection(HIVE_META_DB_URL, HIVE_META_DB_USERNAME,
                    DataUtil.decrypt(HIVE_META_DB_PASSWORD, key));
            if (null == connMysql) {
                logger.warn("DbInfoHelper/getHiveColumnInfo() fail: cannot get database connection.");
                return null;
            }
            String sql = "";
            PreparedStatement pstmt = connMysql.prepareStatement(sql);

            pstmt.setObject(1, tablename);

            ResultSet rs = pstmt.executeQuery(sql);
            while (rs.next()) {
                String column_name = rs.getString("");
                String column_type = rs.getString("");
                String column_info = column_name + "(" + column_type + ")";
                columns.add(column_info);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connMysql.close();
        }
        return columns;
    }
}