package com.james.project.commodity_trace.driver;

import com.james.project.commodity_trace.thread.CommodityThread;

public class CommodyTraceDriver {
    public static void main(String[] args) {
        new CommodityThread().start();
    }
}
