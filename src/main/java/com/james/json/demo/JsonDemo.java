package com.james.json.demo;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonDemo {
    public static void main(String[] args) throws JSONException {
        String denaliLogString = "{\"product_id\":\"\",\"end_time\":0,\"auto_renew\":false,\"purchase_time\":0,\"region\":\"na\",\"connected_svcs_product_type\":0,\"start_time\":0,\"app_id\":\"0ea274d1-f80d-4eff-8dc1-5d632209aa95\",\"order_id\":\"\",\"connected_svcs_purchase_state\":0,\"demo_mode_vehicle_state\":false,\"map_source\":\"HERE\",\"vehicle_manufacturer\":\"34\",\"schema_definition\":\"AppStart\",\"connection_type\":\"WIFI\",\"event_name\":\"APP_START\",\"os_version\":\"6.0.1\",\"language\":\"en\",\"car_model\":\"123415\",\"log_context\":{\"log_id\":\"d92b6d95-0c7d-4cc1-8c9c-25d2a19df367\",\"app_version\":\"4.0.140.3.000\",\"utc_timestamp\":1531221507941,\"time_zone\":\"America/Los_Angeles\",\"visitor_id\":\"c6ab6302-0b1a-4ae5-9af2-1d5634f2d5b7\",\"device_model\":\"TbooK 16 Power(M5F6)\",\"device_make\":\"Teclast\",\"car_id\":\"\",\"current_lat\":42.329645,\"log_version\":\"v2\",\"current_lon\":-83.039008,\"reg_vid\":\"Unknown\"},\"model_year\":\"1\"} response:1000\n" + 
                "";

        JSONObject denaliLog = new JSONObject(denaliLogString);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode payload = mapper.createObjectNode();

        payload.put("payload", denaliLog.toString());

        System.out.println(payload.toString());
    }
}
