package com.james.demo.json.gson.driver;

import com.google.gson.Gson;
import com.james.common.util.JamesUtil;
import com.james.demo.json.gson.entity.Data;
import com.james.demo.json.gson.entity.Device;
import com.james.demo.json.gson.entity.User;
import com.james.demo.json.gson.entity.VoiceCloudData;

public class Main {
    public static void main(String[] args) {
        String strData = makeSampleData();
        System.out.println("Sample data:\n" + strData);
        System.out.println("length:\n" + makeSampleData().getBytes().length);
        JamesUtil.printDivider();

        Gson gson = new Gson();
        VoiceCloudData cloudDataFromString = new VoiceCloudData();
        cloudDataFromString = gson.fromJson(strData, cloudDataFromString.getClass());
        System.out.println("cloudDataFromString: " + cloudDataFromString);
        JamesUtil.printDivider();

        System.out.println("test: " + gson.toJson(cloudDataFromString));
    }

    public static String makeSampleData() {
        // Gson
        Gson gson = new Gson();

        // Device
        Device device = new Device();
        device.setSerial("James-001");
        device.setMac("01-02-03-04-05");
        device.setPos("Shanghai");
        device.setType("Basic001");

        // User
        User user = new User();
        user.setName("James");

        // Data
        Data data = new Data();
        data.setIdx(1);
        data.setIflag(0);
        data.setIlen(11);
        data.setEntity("hello james");

        VoiceCloudData cloudData = new VoiceCloudData();
        cloudData.setMode("1");
        cloudData.setToken("abcdefg");
        cloudData.setDevice(device);
        cloudData.setUser(user);
        cloudData.setData(data);

        String strClouddData = gson.toJson(cloudData);
        return strClouddData;
    }
}
