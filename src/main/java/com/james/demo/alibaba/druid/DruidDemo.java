package com.james.demo.alibaba.druid;

import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.util.JdbcConstants;

public class DruidDemo {

    public static void main(String[] args) {
        // String sql = new StringBuilder(
        // "select parsed_vehicle_sensor_event.logId as
        // log_id,parsed_vehicle_sensor_event.regVid as
        // reg_vid,parsed_vehicle_sensor_event.carId as car_id,'20180321' as
        // date_id,parsed_vehicle_sensor_event.sensorType as
        // sensor_type,parsed_vehicle_sensor_event.timestamp as
        // timestamp,parsed_vehicle_sensor_event.value as
        // value,parsed_vehicle_sensor_event.previous_value as
        // previous_value,parsed_vehicle_sensor_event.status as
        // status,parsed_vehicle_sensor_event.type as
        // type,parsed_vehicle_sensor_event.vehicle_lat as
        // vehicle_lat,parsed_vehicle_sensor_event.vehicle_lon as
        // vehicle_lon,parsed_vehicle_sensor_event.heading as
        // heading,parsed_vehicle_sensor_event.elevation as
        // elevation,parsed_vehicle_sensor_event.speed as
        // speed,parsed_vehicle_sensor_event.precision as
        // precision,parsed_vehicle_sensor_event.mode as mode from
        // vehicle_sensor_event_table_to_parse")
        // .toString();
        //
        // String dbType = JdbcConstants.MYSQL;
        // List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        //
        // ExportTableAliasVisitor visitor = new ExportTableAliasVisitor();
        // for (SQLStatement stmt : stmtList) {
        // stmt.accept(visitor);
        // }
        //
        // SQLTableSource tableSource = visitor.getAliasMap().get("a");
        // System.out.println(tableSource);
        //
        // //
        //
        // StringBuilder out = new StringBuilder();
        // MySqlOutputVisitor visitor2 = new MySqlOutputVisitor(out);
        // MySqlStatementParser parser = new MySqlStatementParser(sql);
        // List<SQLStatement> statementList = parser.parseStatementList();
        // for (SQLStatement statement : statementList) {
        // statement.accept(visitor2);
        // visitor2.println();
        // }

        final String dbType = JdbcConstants.MYSQL; // JdbcConstants.MYSQL或者JdbcConstants.POSTGRESQL
        String sql = "select * from mytable a where a.id = 3";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        ExportTableAliasVisitor visitor = new ExportTableAliasVisitor();
        for (SQLStatement stmt : stmtList) {
            stmt.accept(visitor);
        }

        SQLTableSource tableSource = visitor.getAliasMap().get("a");
        System.out.println(tableSource);
    }
}
