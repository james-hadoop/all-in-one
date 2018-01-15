package com.james.json.demo;

import org.json.JSONException;
import org.json.JSONObject;

public class JsonDemo {
    public static void main(String[] args) throws JSONException {
        // String jsonString =
        // "{\"payload\":{\"k1\":\"v1\",\"k2\":\"v2\"},\"schema_definition\":\"schema_definition\"}";
        String jsonString = "{\"payload\":{\"duration\":1,\"route_id\":\"ceac1961-124b-491e-9ad6-0ad924234ec2\",\"distance\":0,\"caused_by\":\"EXIT\",\"log_context\":{\"log_id\":\"e722269d-c0f5-4e9a-a451-338de3e6e7d2\",\"current_lat\":37.399094,\"utc_timestamp\":1508978706618,\"visitor_id\":\"b09f270c-736f-40c0-965b-f2089936f60d\",\"car_id\":\"\",\"log_version\":\"v2\",\"reg_vid\":\"DenaliMY19_Unknown\",\"current_lon\":-121.9770482},\"event_name\":\"NAV_END\",\"parent_route_id\":\"8a7da039-eb2f-421f-8c82-056f5b96c603\",\"schema_definition\":\"NavEnd\"},\"logshed_app_id\":\"denali_usage_logs_replay\",\"client_address\":\"10.222.224.172\",\"type\":1,\"slogtime\":1511287032829}";
        System.out.println("jsonString:\n\t" + jsonString);

        JSONObject logs = new JSONObject(jsonString);

        String payloadString = logs.getString("payload");
        System.out.println("payloadString=" + payloadString);

        JSONObject payload = new JSONObject(payloadString);
        String defination = payload.getString("schema_definition");
        System.out.println("defination=" + defination);
    }
}
