package com.james.common.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class WeatherUtil {
    public final static String WEATHER_URL = "http://www.weather.com.cn/adat/cityinfo/101020100.html";

    public static String getWeather() throws IOException {
        URL uri = new URL(WEATHER_URL);

        HttpURLConnection conn = null;
        conn = (HttpURLConnection) uri.openConnection();
        if (conn == null) {
            System.out.println("conn == null");
        }
        conn.connect();

        int res = conn.getResponseCode();
        if (res == 200) {
            // response code
            // System.out.println("res == 200");

            // response data
            InputStream is = conn.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);

            String line = null;
            while (null != (line = br.readLine())) {

                int nIndexBegin = line.indexOf("\"temp1\":\"");
                String strTemperature = line.substring(nIndexBegin + 9, nIndexBegin + 9 + 2);
                // System.out.println("strTemperature: " + strTemperature);
                conn.disconnect();
                return strTemperature + "��";
            }
        }
        return null;
    }
}