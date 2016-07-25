package com.james.demo.resource;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import com.james.common.util.HumanReadableUtils;

public class MachineResource {
    public static void main(String[] args) {
        OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long freePhysicalMemorySize = ((com.sun.management.OperatingSystemMXBean) osmxb).getFreePhysicalMemorySize();
        System.out.println("freePhysicalMemorySize=" + freePhysicalMemorySize);
        System.out.println("freePhysicalMemorySize=" + HumanReadableUtils.byteSize(freePhysicalMemorySize));

        long size = 3841921024l;
        String strReadableSize = HumanReadableUtils.byteSizeWithoutUnit(size, false);
        float fReadableSize = Float.parseFloat(strReadableSize);
        System.out.println("strReadableSize=" + strReadableSize);
        System.out.println("fReadableSize=" + fReadableSize);
        System.out.println(fReadableSize > 4);
        System.out.println(fReadableSize < 4);
    }
}
