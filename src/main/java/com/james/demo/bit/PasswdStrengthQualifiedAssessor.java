package com.james.demo.bit;

public class PasswdStrengthQualifiedAssessor {
    private static final int PASSWD_STRENGTH_TYPE_NUMBER = 3;

    public static void main(String[] args) {
        // false
        System.out.println(isPasswdStrengthQualified("HelloWorld"));
         // true
         System.out.println(isPasswdStrengthQualified("HelloWorld12"));
         // true
         System.out.println(isPasswdStrengthQualified("HelloWorld12]"));
    }

    public static boolean isPasswdStrengthQualified(String passwd) {
        if (PASSWD_STRENGTH_TYPE_NUMBER <= calPasswdStrength(passwd)) {
            return true;
        }

        return false;
    }

    private static int calPasswdStrength(String passwd) {
        if (null == passwd || passwd.isEmpty()) {
            return 0;
        }

        int passwdLevel = 0;

        for (int i = 0; i < passwd.length(); i++) {
            passwdLevel |= passwdType(passwd.charAt(i));
        }

        int passwdStrengthTypeContained = 0;
        for (int i = 0; i < Integer.SIZE; i++) {
            passwdStrengthTypeContained += passwdLevel & 1;
            passwdLevel = passwdLevel >> 1;
        }
        
        return passwdStrengthTypeContained;
    }

    private static int passwdType(char c) {
        if (c >= 97 && c <= 122) {
            return 1;
        }
        if (c >= 65 && c <= 90) {
            return 2;
        }
        if (c >= 48 && c <= 57) {
            return 4;
        }
        return 8;
    }
}
