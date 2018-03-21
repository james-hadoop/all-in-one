package com.james.demo.sql_parser;

import java.util.List;

import com.james.common.util.JamesUtil;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.AddAliasesVisitor;

public class SqlParser2 {

    public static void main(String[] args) throws JSQLParserException {
        String sql = new StringBuilder(
                "select parsed_vehicle_sensor_event.logId as log_id,parsed_vehicle_sensor_event.regVid as reg_vid,parsed_vehicle_sensor_event.carId as car_id,'20180321' as date_id,parsed_vehicle_sensor_event.sensorType as sensor_type,parsed_vehicle_sensor_event.timestamp as timestamp,parsed_vehicle_sensor_event.value as value,parsed_vehicle_sensor_event.previous_value as previous_value,parsed_vehicle_sensor_event.status as status,parsed_vehicle_sensor_event.type as type,parsed_vehicle_sensor_event.vehicle_lat as vehicle_lat,parsed_vehicle_sensor_event.vehicle_lon as vehicle_lon,parsed_vehicle_sensor_event.heading as heading,parsed_vehicle_sensor_event.elevation as elevation,parsed_vehicle_sensor_event.speed as speed,parsed_vehicle_sensor_event.precision as precision,parsed_vehicle_sensor_event.mode as mode from vehicle_sensor_event_table_to_parse")
                        .toString();
        System.out.println("sql: \n\t" + sql);

        Select select = (Select) CCJSqlParserUtil.parse(sql);
        final AddAliasesVisitor instance = new AddAliasesVisitor();
        select.getSelectBody().accept(instance);
        System.out.println(select.getSelectBody().toString());
        System.out.println();
        List<WithItem> withItemList = select.getWithItemsList();
        if (null != withItemList) {
            for (WithItem item : withItemList) {
                System.out.println(item);
            }
        }
        System.out.println();
        JamesUtil.printDivider();
    }
}
