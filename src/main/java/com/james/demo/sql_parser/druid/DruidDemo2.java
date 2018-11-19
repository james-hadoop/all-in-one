package com.james.demo.sql_parser.druid;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.stat.TableStat.Name;
import com.alibaba.druid.stat.TableStat.Relationship;
import com.alibaba.druid.util.JdbcConstants;
import com.james.common.util.JamesUtil;

public class DruidDemo2 {

    public static void main(String[] args) {
        String dbType = JdbcConstants.MYSQL;

//        String sql = new StringBuilder(
//                "select parsed_vehicle_sensor_event.logId as log_id,parsed_vehicle_sensor_event.regVid as reg_vid,parsed_vehicle_sensor_event.carId as car_id,'20180321' as date_id,parsed_vehicle_sensor_event.sensorType as sensor_type,parsed_vehicle_sensor_event.timestamp as timestamp,parsed_vehicle_sensor_event.value as value,parsed_vehicle_sensor_event.previous_value as previous_value,parsed_vehicle_sensor_event.status as status,parsed_vehicle_sensor_event.type as type,parsed_vehicle_sensor_event.vehicle_lat as vehicle_lat,parsed_vehicle_sensor_event.vehicle_lon as vehicle_lon,parsed_vehicle_sensor_event.heading as heading,parsed_vehicle_sensor_event.elevation as elevation,parsed_vehicle_sensor_event.speed as speed,parsed_vehicle_sensor_event.precision as precision,parsed_vehicle_sensor_event.mode as mode from vehicle_sensor_event_table_to_parse;")
//                        .toString();
        // String sql = "select a,b,c from t_table";

        String sql = "SELECT at_a.a_a, at_b.b_b\n" + "FROM (\n" + "    SELECT a_key, MAX(a_a) AS a_a, MAX(a_b) AS a_b\n"
                + "        , MAX(a_c) AS a_c\n" + "    FROM t_a\n" + "    GROUP BY a_key\n" + "    ORDER BY a_c\n"
                + ") at_a\n" + "    LEFT JOIN (\n" + "        SELECT b_key, MAX(b_a) AS b_a, MAX(b_b) AS b_b\n"
                + "            , MAX(b_c) AS b_c\n" + "        FROM t_b\n" + "        GROUP BY b_key\n"
                + "        ORDER BY b_c\n" + "    ) at_b\n" + "    ON at_a.a_key = at_b.b_key";

        System.out.println("sql: \n\t" + sql);

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, dbType);

        for (SQLStatement statement : statementList) {
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            statement.accept(visitor);

            // visitor.getColumns():
            System.out.println("\n--> visitor.getColumns():");
            System.out.println(visitor.getColumns());
            
            // visitor.getTables():
            System.out.println("\n--> visitor.getTables():");
            Map<Name, TableStat> tableMap = visitor.getTables();
            Set<Name> setKey = tableMap.keySet();
            for (Name key : setKey) {
                System.out.println("key: " + key.getName() + " --> " + "value: " + tableMap.get(key));
            }
            
            SchemaRepository sr=visitor.getRepository();

//            // visitor.getAggregateFunctions()
//            System.out.println("\n--> visitor.getAggregateFunctions():");
//            List<SQLAggregateExpr> funList = visitor.getAggregateFunctions();
//            for (SQLAggregateExpr fun : funList) {
//                System.out.println(fun.getMethodName());
////                System.out.println("\tgetBeforeCommentsDirect():");
////                JamesUtil.printList(fun.getBeforeCommentsDirect());
//                System.out.println("\tgetAfterCommentsDirect():");
//                JamesUtil.printList(fun.getAfterCommentsDirect());
//            }

            // visitor.getGroupByColumns():
            System.out.println("\n--> visitor.getGroupByColumns():");
            Set<Column> columnSet = visitor.getGroupByColumns();
            for (Column col : columnSet) {
                System.out.println(col.getName() + " -> " + col.getFullName() + " -> " + col.getDataType());
            }

            // visitor.getOrderByColumns():
            System.out.println("\n--> visitor.getOrderByColumns():");
            List<Column> columnList = visitor.getOrderByColumns();
            for (Column col : columnList) {
                System.out.println(col.getName() + " -> " + col.getFullName() + " -> " + col.getDataType());
            }

            // visitor.getRelationships():
            System.out.println("\n--> visitor.getRelationships():");
            Set<Relationship> relationshipSet = visitor.getRelationships();
            for (Relationship rel : relationshipSet) {
                System.out.println(
                        rel.getLeft() + " -> " + rel.getRight() + " -> " + rel.toString() + " -> " + rel.getOperator());
            }
        }
    }
}
