package com.james.demo.basic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListObject {

    public static void main(String[] args) {
        List<Object> list = new ArrayList<Object>();
        list.add(null);

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("1", null);
        map.put(null, "2");
    }
}
