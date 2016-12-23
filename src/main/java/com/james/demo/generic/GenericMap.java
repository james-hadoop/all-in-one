package com.james.demo.generic;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.james.common.util.JamesUtil;

public class GenericMap {
    public static <K, V> Map<K, V> cloneMap(Map<K, V> map) {
        if (null == map || 0 == map.size()) {
            return null;
        }

        Map<K, V> targetMap = new ConcurrentHashMap<K, V>();
        targetMap.putAll(map);

        return targetMap;
    }

    public static void main(String[] args) {
        Map<String, String> map1 = new ConcurrentHashMap<String, String>();
        map1.put("k1", "v1");
        map1.put("k2", "v2");

        for (Entry<String, String> entry : map1.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();

        Map<String, String> map1Clone = GenericMap.cloneMap(map1);
        for (Entry<String, String> entry : map1Clone.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        JamesUtil.printDivider();

        Map<String, Integer> map2 = new ConcurrentHashMap<String, Integer>();
        map2.put("k11", 1);
        map2.put("k12", 2);

        for (Entry<String, Integer> entry : map2.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();

        Map<String, Integer> map2Clone = GenericMap.cloneMap(map2);
        for (Entry<String, Integer> entry : map2Clone.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
    }
}
