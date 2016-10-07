package com.james.common.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUtil.class);

    // public static final long MB_SIZE = 1024 * 1024;
    public static final long MB_SIZE = 1024 * 1024;

    /**
     * free命令：用于查看内存使用情况
     */
    public static final String FREE_COMMAND = "free -m";

    public static double getMachineMem(String commandStr) throws Exception {
        double freeMem = ResourceUtil.getFreePhysicalMemorySize();
        BufferedReader bufferedReader = null;
        try {
            Runtime rt = Runtime.getRuntime();
            Process p = rt.exec(commandStr);// 执行相关命令
            bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                LOGGER.info(str);
                if (str.indexOf("buffers/cache") != -1) {
                    LOGGER.info("with cache {}", str);
                    str = str.substring(str.indexOf(":") + 2).trim();
                    String parts[] = str.split("\\s+");
                    int used = Integer.valueOf(parts[0]);
                    freeMem = Double.valueOf(parts[1]);
                }
            }
        } catch (Exception e) {
            LOGGER.error("get system free mem error.", e);
        } finally {
            bufferedReader.close();
        }
        return freeMem;
    }

    private static OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    /**
     * Get machine free physical memory Mb unit
     * 
     * @return
     */
    public static double getFreePhysicalMemorySize() {
        long freeMem = ((com.sun.management.OperatingSystemMXBean) osmxb).getFreePhysicalMemorySize();
        return (double) freeMem / MB_SIZE;
    }

    /**
     * Get machine total physical memory
     * 
     * @return
     */
    public static double getTotalPhysicalMemorySize() {
        long total = ((com.sun.management.OperatingSystemMXBean) osmxb).getTotalPhysicalMemorySize();
        return (double) total / MB_SIZE;
    }

    /**
     * Parse jvm parameter
     * 
     * eg. "-Xms1024m -Xmx4096m" eg. "-Xms1g -Xmx4g"
     * 
     * @return
     */
    public static int parseJavaEnv(String input) {
        Integer ret = 0;
        try {
            String patternText1 = ".*-Xmx";

            Pattern pattern = Pattern.compile(patternText1);
            Matcher matcher = pattern.matcher(input);

            String mid = matcher.replaceAll("");
            if (mid.toLowerCase().endsWith("m")) {
                ret = Integer.valueOf(mid.substring(0, mid.lastIndexOf("m")));
            } else if (mid.toLowerCase().endsWith("g")) {
                ret = Integer.valueOf(mid.substring(0, mid.lastIndexOf("g"))) * 1024;
            }

        } catch (Exception e) {
        }
        return ret;
    }

    public static String getJavaEnv(String input, int javaDefaultMemorySize) {
        String javaOpts = "-Xmx" + javaDefaultMemorySize + "m -Xms" + javaDefaultMemorySize + "m";
        try {
            if (DataUtil.isNullOrEmpty(input)) {
                return javaOpts;
            }

            Integer needOpts = ResourceUtil.parseJavaEnv(input);

            if (needOpts.intValue() <= 1024) {
                return javaOpts;
            }

        } catch (Exception e) {
            return javaOpts;
        }
        return input;
    }
}
