package com.james.common.util;

import java.util.List;

import com.james.common.util.entity.DataMysqlTableField;
import com.james.common.util.entity.SqlTableField;

public class HadoopScriptUtil {
    public static String generateHiveDDL(String hiveDbName, String hiveTableName, String hiveTableComment,
            List<DataMysqlTableField> listMysqlTableField) {
        if (null == hiveDbName || 0 == hiveDbName.length() || null == hiveTableName || 0 == hiveTableName.length()
                || null == listMysqlTableField || 0 == listMysqlTableField.size()) {
            return null;
        }

        StringBuilder sql = new StringBuilder("create table if not exists " + hiveDbName + "." + hiveTableName + " ("
                + "\n");

        SqlTableField stf = null;
        for (DataMysqlTableField dmtf : listMysqlTableField) {
            stf = new SqlTableField(dmtf.getFieldName(), mysqlType2HiveType(dmtf.getDataType()), dmtf.getComment());
            sql.append(stf.toString() + ",\n");
        }

        sql = sql.delete(sql.length() - 2, sql.length());

        sql.append(")\ncomment '" + hiveTableComment + "'\npartitioned by (dt String)\nstored as TEXTFILE");

        return sql.toString();
    }

    public static String generateSqoopImport(String mysqlDbName, String mysqlTableName, String mysqlUsername,
            String mysqlUrl, String hiveDbName, String hiveTableName) {
        if (null == hiveDbName || 0 == hiveDbName.length() || null == hiveTableName || 0 == hiveTableName.length()) {
            return null;
        }

        String sql = "import --connect "
                + mysqlUrl
                + "?zeroDateTimeBehavior=round&tinyInt1isBit=false"
                + " --driver com.mysql.jdbc.Driver --username "
                + mysqlUsername
                + " --password-file /user/oozie/share/lib/lib_20151229093730/password_mysql_pro.file --table "
                + mysqlTableName
                + " --hcatalog-database "
                + hiveDbName
                + " --hcatalog-table "
                + hiveTableName
                + " --hcatalog-partition-keys dt --hcatalog-partition-values ${dt} --hive-drop-import-delims --skip-dist-cache";

        return sql;
    }

    private static String mysqlType2HiveType(String mysqlType) {
        // tinyint, tinyint(n) unsigned ==> smallint, tinyint(n) signed ==>
        // tinyint
        if (mysqlType.matches("^tinyint(.*)unsigned(.*)")) {
            return "smallint";
        } else if (mysqlType.matches("^tinyint(.*)\\){1}(.*)(signed)?")) {
            return "tinyint";
        }
        // smallint,smallint(n) unsigned ==> int, smallint(n) signed ==>
        // smallint
        else if (mysqlType.matches("^smallint(.*)unsigned(.*)")) {
            return "int";
        } else if (mysqlType.matches("^smallint(.*)\\){1}(.*)(signed)?")) {
            return "smallint";
        }
        // mediumint ==> int
        else if (mysqlType.matches("^mediumint(.*)unsigned(.*)")) {
            return "int";
        } else if (mysqlType.matches("^mediumint(.*)\\){1}(.*)(signed)?")) {
            return "int";
        }
        // int, int(n) unsigned ==> bigint, int(n) signed ==> int
        else if (mysqlType.matches("^int(.*)unsigned(.*)")) {
            return "bigint";
        } else if (mysqlType.matches("^int(.*)\\){1}(.*)(signed)?")) {
            return "int ";
        }
        // bigint, bigint(n) unsigned ==> String, bigint(n) signed ==> bigint
        else if (mysqlType.matches("^bigint(.*)unsigned(.*)")) {
            return "String";
        } else if (mysqlType.matches("^bigint(.*)\\){1}(.*)(signed)?")) {
            return "bigint";
        }
        // decimal==>String
        else if (mysqlType.matches("^decimal(.*)")) {
            return "String";
        }
        // numeric==>String
        else if (mysqlType.matches("^numeric(.*)")) {
            return "String";
        }
        // float,float(M,N)==>String
        else if (mysqlType.matches("^float(.*)")) {
            return "String";
        }
        // double,double precision, double(M,N), double precision(M,N) ==>
        // String
        else if (mysqlType.matches("^double(.*)")) {
            return "String";
        }
        // real, real(M,N) ==> String
        else if (mysqlType.matches("^real(.*)")) {
            return "String";
        }
        // bit, bit(M) ==> String
        else if (mysqlType.matches("^bit(.*)")) {
            return "String";
        }
        // date ==> String
        else if (mysqlType.matches("^date$")) {
            return "String";
        }
        // datetime, datetime(n), n=0~6 ==> String
        else if (mysqlType.matches("^datetime(.*)\\)?")) {
            return "String";
        }
        // timestamp,timestamp(n), n=0~6 ==> String
        else if (mysqlType.matches("^timestamp(.*)\\)?")) {
            return "String";
        }
        // time, time(n) ==> String
        else if (mysqlType.matches("^time\\(?(\\d*)\\)?")) {
            return "String";
        }
        // year, year(4) ==> int
        else if (mysqlType.matches("^year(.*)")) {
            return "int";
        }
        // char(n) ==> char(n)
        else if (mysqlType.matches("^char(.*)")) {
            return mysqlType;
        }
        // varchar(n) ==> varchar(n)
        else if (mysqlType.matches("^varchar(.*)")) {
            return mysqlType;
        }
        // binary(n) ==> String
        else if (mysqlType.matches("^binary(.*)")) {
            return "String";
        }
        // varchar(n) ==> String
        else if (mysqlType.matches("^varbinary(.*)")) {
            return "String";
        }
        // tinyblob ==> String
        else if (mysqlType.matches("^tinyblob(.*)")) {
            return "String";
        }
        // blob ==> String
        else if (mysqlType.matches("^blob(.*)")) {
            return "String";
        }
        // mediumblob ==> String
        else if (mysqlType.matches("^mediumblob(.*)")) {
            return "String";
        }
        // longblob ==> String
        else if (mysqlType.matches("^longblob(.*)")) {
            return "String";
        }
        // tinytext ==> String
        else if (mysqlType.matches("^tinytext(.*)")) {
            return "String";
        }
        // text ==> String
        else if (mysqlType.matches("^text(.*)")) {
            return "String";
        }
        // mediumtext ==> String
        else if (mysqlType.matches("^mediumtext(.*)")) {
            return "String";
        }
        // longtext ==> String
        else if (mysqlType.matches("^longtext(.*)")) {
            return "String";
        }
        // enum ==> String
        else if (mysqlType.matches("^enum(.*)")) {
            return "String";
        }
        // set ==> String
        else if (mysqlType.matches("^set(.*)")) {
            return "String";
        } else {
            return null;
        }
    }
}
