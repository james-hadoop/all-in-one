package com.james.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.security.Key;
import java.security.MessageDigest;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import org.apache.commons.codec.binary.Base64;

public class DataUtil {
    private final static byte[] KEY = "".getBytes();
    private static final String KEY_ALGORITHM = "DES";
    private static final String CIPHER_ALGORITHM = "DES/ECB/PKCS5Padding";

    private static char hexdigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    private static final String KEY_DIVIDER = "\\|";
    private static final String KEY_DIVIDER_FOR_AGGREGATION = "|";
    private static final String KEY_VALUE_DIVIDER = ":";

    public static boolean isNullOrEmpty(String input) {
        if (input == null || input.length() <= 0) {
            return true;
        }
        return false;
    }

    public static boolean isNumber(String str) {
        boolean ret = false;
        if (isNullOrEmpty(str)) {
            ret = false;
        } else {
            Pattern pattern = Pattern.compile("[0-9]*");
            ret = pattern.matcher(str).matches();
        }
        return ret;
    }

    public static boolean isContainChinese(String str) {
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(str);
        if (m.find()) {
            return true;
        }
        return false;
    }

    public static Double getDoubleWith2AfterPoint(Double input) {
        Double ret = 0d;
        try {
            DecimalFormat df = new DecimalFormat("#.00");
            ret = Double.valueOf(df.format(input));
        } catch (Exception e) {
        }
        return ret;
    }

    public static Double getDoubleWith4AfterPoint(Double input) {
        Double ret = 0d;
        try {
            DecimalFormat df = new DecimalFormat("#.0000");
            ret = Double.valueOf(df.format(input));
        } catch (Exception e) {
        }
        return ret;
    }

    public static void printList(List<String> listString) {
        for (String str : listString) {
            System.out.println(str);
        }
    }

    public static List<byte[]> byte2List(byte[] data, int size) {
        List<byte[]> listByte = new ArrayList<byte[]>();

        byte[] byteData = new byte[size];

        int len = data.length;

        int start = 0;
        int end = size;

        while (end <= len) {
            for (int i = 0; i < size; i++) {
                byteData[i] = data[start + i];
            }
            listByte.add(byteData);

            start = end;
            end += size;
        }

        return listByte;
    }

    public static void bianLi2dArray(String[][] arrArrString) {
        int row = arrArrString.length;
        int column = arrArrString[0].length;

        for (int i = 0; i < row; i++) {
            for (int j = 0; j < column; j++) {
                System.out.print(arrArrString[i][j] + "        ");
            }
            System.out.println();
        }
    }

    public static List<String> string2List(String data, int size) {
        List<String> listString = new ArrayList<String>();

        int len = data.length();

        int start = 0;
        int end = size;

        while (end <= len) {
            String strPart = data.substring(start, end);
            listString.add(strPart);

            start = end;
            end += size;
        }

        return listString;
    }

    public static void bianLiList(ArrayList<String> listString) {
        int size = listString.size();
        System.out.println("\nsize: " + size);

        for (int i = 0; i < size; i++) {
            System.out.print(listString.get(i) + "\t");
        }
    }

    public static int bytes2Int(byte[] data) {
        int targets = (data[0] & 0xff) | ((data[1] << 8) & 0xff00) | ((data[2] << 24) >>> 8) | (data[3] << 24);
        return targets;
    }

    public static byte[] int2Bytes(int data) {
        byte[] bytes = new byte[4];

        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data >> 8) & 0xff);
        bytes[2] = (byte) ((data >> 16) & 0xff);
        bytes[3] = (byte) (data >>> 24);
        return bytes;
    }

    private static Key toKey(byte[] key) throws Exception {
        DESKeySpec dks = new DESKeySpec(key);

        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);

        SecretKey secretKey = keyFactory.generateSecret(dks);

        return secretKey;
    }

    public static String decrypt(String data, byte[] key) throws Exception {
        Key k = toKey(key);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        cipher.init(Cipher.DECRYPT_MODE, k);

        return new String(cipher.doFinal(Base64.decodeBase64(data)));
    }

    public static String encrypt(String data, byte[] key) throws Exception {
        Key k = toKey(key);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        cipher.init(Cipher.ENCRYPT_MODE, k);

        return Base64.encodeBase64String(cipher.doFinal(data.getBytes()));
    }

    public static String decrypt(String data) throws Exception {
        Key k = toKey(KEY);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        cipher.init(Cipher.DECRYPT_MODE, k);

        return new String(cipher.doFinal(Base64.decodeBase64(data)));
    }

    public static String encrypt(String data) throws Exception {
        Key k = toKey(KEY);

        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        cipher.init(Cipher.ENCRYPT_MODE, k);

        return Base64.encodeBase64String(cipher.doFinal(data.getBytes()));
    }

    public static byte[] initKey() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance(KEY_ALGORITHM);

        kg.init(56);

        SecretKey secretKey = kg.generateKey();

        return secretKey.getEncoded();
    }

    public static String getMD5(File file) throws Exception {

        FileInputStream fis = null;

        MessageDigest md = MessageDigest.getInstance("MD5");
        fis = new FileInputStream(file);
        byte[] buffer = new byte[2048];

        int length = -1;

        while ((length = fis.read(buffer)) != -1) {
            md.update(buffer, 0, length);
        }
        fis.close();
        byte[] b = md.digest();
        return byteToHexString(b);
    }

    private static String byteToHexString(byte[] tmp) {
        String s;
        // 用字节表示就是 16 个字节
        char str[] = new char[16 * 2]; // 每个字节用 16 进制表示的话，使用两个字符，所以表示成 16 进制需要
                                       // 32 个字符
        int k = 0; // 表示转换结果中对应的字符位置
        for (int i = 0; i < 16; i++) { // 从第一个字节开始，对 MD5 的每一个字节
            // 转换成 16 进制字符的转换
            byte byte0 = tmp[i]; // 取第 i 个字节
            str[k++] = hexdigits[byte0 >>> 4 & 0xf]; // 取字节中高 4 位的数字转换, >>>
                                                     // 为逻辑右移，将符号位一起右移
            str[k++] = hexdigits[byte0 & 0xf]; // 取字节中低 4 位的数字转换
        }

        s = new String(str); // 换后的结果转换为字符串

        return s;
    }

    /**
     * "k1:v1|k2:v2" --> Map
     * 
     * @param text
     * @return
     */
    public static Map<String, Object> string2Map(String text) {
        if (null == text || 0 == text.length()) {
            return null;
        }

        Map<String, Object> map = new HashMap<String, Object>();

        String[] arrKeys = text.split(KEY_DIVIDER);

        for (String keys : arrKeys) {
            String[] arrKV = keys.split(KEY_VALUE_DIVIDER);

            if (2 == arrKV.length) {
                map.put(arrKV[0], arrKV[1]);
            } else {
                map.put(arrKV[0], "");
            }
        }

        return map;
    }

    /**
     * Map --> "k1:v1|k2:v2"
     * 
     * @param map
     * @return
     */
    public static String map2String(Map<String, Object> map) {
        if (null == map || 0 == map.size()) {
            return null;
        }

        StringBuilder sb = new StringBuilder();

        Set<String> setKey = map.keySet();
        for (String key : setKey) {
            sb.append(key + KEY_VALUE_DIVIDER + map.get(key) + KEY_DIVIDER_FOR_AGGREGATION);
        }

        return sb.substring(0, sb.length() - 1).toString();
    }

    public static List<String> string2List(String str, String token) {
        List<String> listString = new ArrayList<String>();

        if (null == str || 0 == str.length()) {
            return listString;
        }

        String[] arrStr = str.split(token);
        for (String s : arrStr) {
            listString.add(s);
        }

        return listString;
    }

    public static String list2String(List<String> listStr, String token) {
        if (null == listStr || 0 == listStr.size()) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String str : listStr) {
            sb.append(str + token);
        }

        return sb.substring(0, sb.length() - 1);
    }
}
