package com.james.demo.enums;

public class Test {
    public static void main(String[] args) {
        Light light = Light.男;
        System.out.println(light);

        int en = RestfulMonitorEnum.getCode("已停止");
        System.out.println("en=" + en);

        int en1 = RestfulCheckEnum.getCode("异常");
        System.out.println("en1=" + en1);

        System.out.println("-----------------------------------------------------");
        System.out.println(DeltaServiceVersion.values());
        for (final DeltaServiceVersion version : DeltaServiceVersion.values()) {
            System.out.println(version);
            System.out.println(version.getVersion());
            System.out.println("--");
            for (final Class<?> cls : version.getServiceClasses()) {
                System.out.println(cls);
            }
            System.out.println();
        }
    }
}
