package com.james.demo.sql_parser.druid;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.util.JdbcConstants;

public class DruidDemo {

    public static void main(String[] args) {
        String dbType = JdbcConstants.MYSQL;

//        String sql = new StringBuilder(
//                "select parsed_vehicle_sensor_event.logId as log_id,parsed_vehicle_sensor_event.regVid as reg_vid,parsed_vehicle_sensor_event.carId as car_id,'20180321' as date_id,parsed_vehicle_sensor_event.sensorType as sensor_type,parsed_vehicle_sensor_event.timestamp as timestamp,parsed_vehicle_sensor_event.value as value,parsed_vehicle_sensor_event.previous_value as previous_value,parsed_vehicle_sensor_event.status as status,parsed_vehicle_sensor_event.type as type,parsed_vehicle_sensor_event.vehicle_lat as vehicle_lat,parsed_vehicle_sensor_event.vehicle_lon as vehicle_lon,parsed_vehicle_sensor_event.heading as heading,parsed_vehicle_sensor_event.elevation as elevation,parsed_vehicle_sensor_event.speed as speed,parsed_vehicle_sensor_event.precision as precision,parsed_vehicle_sensor_event.mode as mode from vehicle_sensor_event_table_to_parse;")
//                        .toString();
        // String sql = "select a,b,c from t_table";

//        String sql = "SELECT tt.a_a, tt.b_b\n" + 
//                "FROM (\n" + 
//                "    SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b\n" + 
//                "        , MAX(a_c) AS a_c\n" + 
//                "    FROM t_a\n" + 
//                "    GROUP BY a_key\n" + 
//                "    ORDER BY a_a\n" + 
//                ") at_a\n" + 
//                "    LEFT JOIN (\n" + 
//                "        SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b\n" + 
//                "            , MAX(b_c) AS b_c\n" + 
//                "        FROM t_b\n" + 
//                "        GROUP BY b_key\n" + 
//                "        ORDER BY b_b\n" + 
//                "    ) at_b\n" + 
//                "    ON at_a.a_key = at_b.b_key AS tt";
        
        String sql = "select * from mytable as a where a.id = 3";

        System.out.println("sql: \n\t" + sql);

        System.out.println("--> parseStatements(sql, dbType)");
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, dbType);

        ExportTableAliasVisitor visitor = new ExportTableAliasVisitor();
        for (SQLStatement stmt : statementList) {
            stmt.accept(visitor);
            
            SQLTableSource tableSource = visitor.getAliasMap().get("a");
            System.out.println(tableSource);
        }


    }

    public static class ExportTableAliasVisitor extends MySqlASTVisitorAdapter {
        private Map<String, SQLTableSource> aliasMap = new HashMap<String, SQLTableSource>();

        public boolean visit(SQLExprTableSource x) {
            String alias = x.getAlias();
            aliasMap.put(alias, x);
            return true;
        }

        public Map<String, SQLTableSource> getAliasMap() {
            return aliasMap;
        }
    }
}
